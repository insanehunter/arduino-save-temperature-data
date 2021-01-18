import os

import requests
from dotenv import load_dotenv

load_dotenv()


class Giphy:
    @staticmethod
    def random_video_url(tag: str) -> str:
        response = requests.get(f'https://api.giphy.com/v1/gifs/random?api_key={os.getenv("GIPHY_API_KEY")}&tag={tag}')
        return response.json()['data']['image_mp4_url']
