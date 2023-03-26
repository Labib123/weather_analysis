import os
import requests


class OpenWeatherMapClient:
    """
    A client for the OpenWeatherMap API.
    """

    def __init__(self):
        self.api_key = os.environ.get("OPENWEATHERMAP_API_KEY")
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"

    def get_weather_data(self, city: str) -> dict:
        """
        Fetches weather data for a given city from the OpenWeatherMap API.
        """
        url = f"{self.base_url}?q={city}&appid={self.api_key}&units=metric"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
