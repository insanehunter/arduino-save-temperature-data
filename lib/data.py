from datetime import datetime, timezone
from enum import Enum
from typing import List, Tuple

import numpy as np
import pykalman
import statsmodels.api as sm
from influxdb import InfluxDBClient


#
# Alerts
#

def get_last_alert_status_and_timestamp(influxdb: InfluxDBClient) -> Tuple[bool, datetime]:
    results = list(influxdb.query(
        'SELECT * FROM temperatures.autogen.alert ORDER BY time DESC LIMIT 1').get_points())
    if not results:
        return False, datetime.utcfromtimestamp(0).replace(tzinfo=timezone.utc)
    return results[-1]['status'] != 'off', _parse_timestamp_str(results[-1]['time'])


def save_alert_status(influxdb: InfluxDBClient, is_on: bool) -> None:
    influxdb.write_points([f'alert,status={"on" if is_on else "off"} value=0'], protocol='line', time_precision='ms')


#
# Watchers
#

def set_watcher_enabled(influxdb: InfluxDBClient, chat_id: str, enabled: bool) -> None:
    influxdb.write_points([f'watcher,status={"on" if enabled else "off"},chat_id={chat_id} value=0'],
                          protocol='line', time_precision='ms')


def get_watcher_enabled(influxdb: InfluxDBClient, chat_id: str):
    results = list(influxdb.query(
        'SELECT * FROM temperatures.autogen.watcher'
        f' WHERE chat_id=\'{chat_id}\' ORDER BY time DESC LIMIT 1').get_points())
    return not results or results[-1]['status'] != 'off'


#
# Measurements
#

def save_measurements(influxdb: InfluxDBClient, measurements: List[Tuple[str, float]]) -> None:
    # Write raw data points
    now = datetime.now().timestamp()
    latest_clock_time = None
    temperature_timestamps = []
    for key, temperature in measurements:
        epoch, clock_time = key.split('_')
        clock_time = int(epoch) * (2 ** 32) + int(clock_time)
        if latest_clock_time is None:
            latest_clock_time = clock_time
        timestamp = int(now * 1000 - (latest_clock_time - clock_time))
        temperature_timestamps.append((temperature, timestamp))
    temperature_timestamps.reverse()
    influxdb.write_points([f'temperature value={temp} {ts}' for temp, ts in temperature_timestamps],
                          protocol='line', time_precision='ms')

    # Updating kalman filter data
    mean, cov = get_kalman_mean_cov(influxdb)
    kf = pykalman.AdditiveUnscentedKalmanFilter()
    lines = []
    mean, cov = [mean], [cov]
    for temp, ts in temperature_timestamps:
        mean, cov = kf.filter_update(mean, cov, temp)
        lines.append(f'temperature_filtered value={mean.item()} {ts}')
    save_kalman_mean_cov(influxdb, mean.item(), cov.item())
    influxdb.write_points(lines, protocol='line', time_precision='ms')


def get_kalman_mean_cov(influxdb: InfluxDBClient) -> Tuple[float, float]:
    results = list(influxdb.query(
        'SELECT * FROM temperatures.autogen.kalman ORDER BY time DESC LIMIT 1').get_points())
    if results:
        mean = results[-1]['mean']
        cov = results[-1]['cov']
    else:
        # Initializing Kalman filter
        temperatures = list(influxdb.query(
            'SELECT value FROM temperatures.autogen.temperature WHERE time > now()-120m'
        ).get_points())
        kf = pykalman.AdditiveUnscentedKalmanFilter()
        filtered_state_means, filtered_state_covariances = kf.filter([t['value'] for t in temperatures])
        mean = filtered_state_means[-1].item()
        cov = filtered_state_covariances[-1].item()
        save_kalman_mean_cov(influxdb, mean, cov)
    return mean, cov


def save_kalman_mean_cov(influxdb: InfluxDBClient, mean: float, cov: float) -> None:
    influxdb.write_points([f'kalman mean={mean},cov={cov}'], protocol='line', time_precision='ms')


def get_last_measurement_timestamp(influxdb: InfluxDBClient) -> datetime:
    results = list(influxdb.query(
        'SELECT * FROM temperatures.autogen.temperature ORDER BY time DESC LIMIT 1').get_points())
    if not results:
        return datetime.utcfromtimestamp(0).replace(tzinfo=timezone.utc)
    return _parse_timestamp_str(results[-1]['time'])


#
# Status
#

def get_current_temperature(influxdb: InfluxDBClient) -> float:
    results = list(influxdb.query(
        'SELECT * FROM temperatures.autogen.temperature_filtered ORDER BY time DESC LIMIT 1').get_points())
    if not results:
        return 0
    return _measurement_to_temperature(results[-1]['value'])


class FurnaceStatus(Enum):
    HEATING_UP = 'HEATING_UP'
    COOLING_DOWN = 'COOLING_DOWN'
    NOT_TRENDING = 'NOT_TRENDING'


def get_furnace_status(influxdb: InfluxDBClient) -> FurnaceStatus:
    temps = list(influxdb.query(
        'SELECT value FROM temperatures.autogen.temperature_filtered WHERE time > now()-3m'
    ).get_points())
    if temps:
        ys = np.array([e['value'] for e in temps])
        xs = sm.add_constant(np.array(range(ys.size)), prepend=False)
        result = sm.OLS(ys, xs).fit()
        if result.rsquared >= 0.8:
            return FurnaceStatus.COOLING_DOWN if result.conf_int()[0].mean() < 0 else \
                FurnaceStatus.HEATING_UP if result.conf_int()[0].mean() > 0 else FurnaceStatus.NOT_TRENDING
    return FurnaceStatus.NOT_TRENDING


#
# Congrats
#

def get_last_congrat_timestamp(influxdb: InfluxDBClient) -> datetime:
    results = list(influxdb.query(
        'SELECT * FROM temperatures.autogen.congrat ORDER BY time DESC LIMIT 1').get_points())
    if not results:
        return datetime.utcfromtimestamp(0).replace(tzinfo=timezone.utc)
    return _parse_timestamp_str(results[-1]['time'])


def save_congrat(influxdb: InfluxDBClient, temperature: float) -> None:
    influxdb.write_points([f'congrat value={temperature}'], protocol='line', time_precision='ms')


#
# Misc
#

def _parse_timestamp_str(timestamp_str: str) -> datetime:
    format_str = '%Y-%m-%dT%H:%M:%S.%fZ' if len(timestamp_str) > 20 else '%Y-%m-%dT%H:%M:%SZ'
    return datetime.strptime(timestamp_str, format_str).replace(tzinfo=timezone.utc)


def _measurement_to_temperature(measurement: float) -> float:
    return measurement + 17
