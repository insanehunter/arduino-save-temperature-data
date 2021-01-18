from datetime import datetime, timezone
from enum import Enum
from typing import List, Tuple

import numpy as np
import statsmodels.api as sm
from influxdb import InfluxDBClient


def _measurement_to_temperature(measurement: float) -> float:
    return measurement + 17


def get_last_alert_status_and_timestamp(influxdb: InfluxDBClient) -> Tuple[bool, datetime]:
    results = list(influxdb.query(
        'SELECT * FROM temperatures.autogen.alert ORDER BY time DESC LIMIT 1').get_points())
    if not results:
        return False, datetime.utcfromtimestamp(0).replace(tzinfo=timezone.utc)
    return results[-1]['status'] != 'off', _parse_timestamp_str(results[-1]['time'])


def save_alert_status(influxdb: InfluxDBClient, is_on: bool) -> None:
    influxdb.write_points([f'alert,status={"on" if is_on else "off"} value=0'], protocol='line', time_precision='ms')


def set_watcher_enabled(influxdb: InfluxDBClient, chat_id: str, enabled: bool) -> None:
    influxdb.write_points([f'watcher,status={"on" if enabled else "off"},chat_id={chat_id} value=0'],
                          protocol='line', time_precision='ms')


def get_watcher_enabled(influxdb: InfluxDBClient, chat_id: str):
    results = list(influxdb.query(
        'SELECT * FROM temperatures.autogen.watcher'
        f' WHERE chat_id=\'{chat_id}\' ORDER BY time DESC LIMIT 1').get_points())
    return not results or results[-1]['status'] != 'off'


def save_measurements(influxdb: InfluxDBClient, measurements: List[Tuple[str, float]]) -> None:
    now = datetime.now().timestamp()
    latest_clock_time = None
    data_points = []
    for key, temperature in measurements:
        epoch, clock_time = key.split('_')
        clock_time = int(epoch) * (2 ** 32) + int(clock_time)
        if latest_clock_time is None:
            latest_clock_time = clock_time
        timestamp = int(now * 1000 - (latest_clock_time - clock_time))
        data_points.append(f'temperature value={temperature} {timestamp}')
    data_points.reverse()
    influxdb.write_points(data_points, protocol='line', time_precision='ms')


def get_last_measurement_timestamp(influxdb: InfluxDBClient) -> datetime:
    results = list(influxdb.query(
        'SELECT * FROM temperatures.autogen.temperature ORDER BY time DESC LIMIT 1').get_points())
    if not results:
        return datetime.utcfromtimestamp(0).replace(tzinfo=timezone.utc)
    return _parse_timestamp_str(results[-1]['time'])


class FurnaceStatus(Enum):
    HEATING_UP = 'HEATING_UP'
    COOLING_DOWN = 'COOLING_DOWN'
    NOT_TRENDING = 'NOT_TRENDING'


def get_current_temperature(influxdb: InfluxDBClient) -> float:
    emas = list(influxdb.query(
        'SELECT EXPONENTIAL_MOVING_AVERAGE(value, 5) AS ema'
        ' FROM temperatures.autogen.temperature WHERE time > now()-7m'
    ).get_points())
    if not emas:
        return 0
    return _measurement_to_temperature(emas[-1]['ema'])


def get_furnace_status(influxdb: InfluxDBClient) -> FurnaceStatus:
    emas = list(influxdb.query(
        'SELECT EXPONENTIAL_MOVING_AVERAGE(value, 5) AS ema'
        ' FROM temperatures.autogen.temperature WHERE time > now()-3m'
    ).get_points())
    if emas:
        ys = np.array([e['ema'] for e in emas])
        xs = sm.add_constant(np.array(range(ys.size)), prepend=False)
        result = sm.OLS(ys, xs).fit()
        if result.rsquared > 0.8:
            return FurnaceStatus.COOLING_DOWN if result.conf_int()[0][1] < 0 else \
                FurnaceStatus.HEATING_UP if result.conf_int()[0][0] > 0 else FurnaceStatus.NOT_TRENDING
    return FurnaceStatus.NOT_TRENDING


def get_last_congrat_timestamp(influxdb: InfluxDBClient) -> datetime:
    results = list(influxdb.query(
        'SELECT * FROM temperatures.autogen.congrat ORDER BY time DESC LIMIT 1').get_points())
    if not results:
        return datetime.utcfromtimestamp(0).replace(tzinfo=timezone.utc)
    return _parse_timestamp_str(results[-1]['time'])


def save_congrat(influxdb: InfluxDBClient, temperature: float) -> None:
    influxdb.write_points([f'congrat value={temperature}'], protocol='line', time_precision='ms')


def _parse_timestamp_str(timestamp_str: str) -> datetime:
    format_str = '%Y-%m-%dT%H:%M:%S.%fZ' if len(timestamp_str) > 20 else '%Y-%m-%dT%H:%M:%SZ'
    return datetime.strptime(timestamp_str, format_str).replace(tzinfo=timezone.utc)
