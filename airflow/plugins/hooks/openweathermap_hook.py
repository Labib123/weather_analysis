import requests


class OpenWeatherMapHook:
    """
    Airflow hook for interacting with the OpenWeatherMap API.
    """

    def __init__(self, api_key):
        """
        Constructor that takes an API key as an argument.
        """
        self.api_key = api_key

    def fetch_weather_data(self, city_name, start_date, end_date):
        """
        Fetches weather data for a given city and date range from the OpenWeatherMap API.
        """
        url = f"https://api.openweathermap.org/data/2.5/forecast?q={city_name}&appid={self.api_key}"
        response = requests.get(url)
        response_json = response.json()

        weather_data = []
        for forecast in response_json["list"]:
            date = forecast["dt_txt"]
            if start_date <= date <= end_date:
                temperature = forecast["main"]["temp"]
                humidity = forecast["main"]["humidity"]
                weather_data.append((city_name, date, temperature, humidity))

        return weather_data
