import requests
import time
import logging
import configparser

# Configurer le logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WeatherProducer")

# Charger la configuration
config = configparser.ConfigParser()
config.read("config.ini")

API_URL = config["API"]["url"]
FETCH_INTERVAL = int(config["API"]["fetch_interval"])
LOG_FILE = config["API"]["log_file"]

# Fonction pour récupérer les données météo
def fetch_weather_data():
    try:
        response = requests.get(API_URL)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to fetch data. Status code: {response.status_code}")
            return {}
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        return {}

# Fonction pour écrire les données dans un fichier texte
def write_to_file(data):
    with open(LOG_FILE, "a") as file:
        try:
            current_weather = data["current_weather"]
            log_message = (
                f"Temperature: {current_weather['temperature']}°C, "
                f"Wind Speed: {current_weather['windspeed']} km/h, "
                f"Condition: {current_weather['weathercode']}"
            )
            file.write(log_message + "\n")
            logger.info(f"Logged: {log_message}")
        except KeyError as e:
            logger.error(f"Error processing data: {e}")

# Boucle pour extraire et stocker les données périodiquement
if __name__ == "__main__":
    while True:
        logger.info("Fetching weather data...")
        data = fetch_weather_data()
        write_to_file(data)
        logger.info(f"Waiting {FETCH_INTERVAL} seconds for the next fetch...")
        time.sleep(FETCH_INTERVAL)
