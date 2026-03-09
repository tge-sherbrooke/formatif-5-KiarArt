# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "adafruit-io",
#   "adafruit-blinka",
#   "adafruit-circuitpython-ahtx0",
#   "rpi.gpio",
# ]
# ///
"""
Publication des donnees de capteurs vers Adafruit IO
Cours 243-413-SH, Semaine 5
"""

from Adafruit_IO import MQTTClient
import board
import adafruit_ahtx0
import time

from config import ADAFRUIT_IO_USERNAME, ADAFRUIT_IO_KEY

# Callbacks MQTT
def connected(client):
    print("Connecte a Adafruit IO!")

def disconnected(client):
    print("Deconnecte de Adafruit IO")

class SensorPublisher:
    """Publie les donnees des capteurs vers Adafruit IO."""

    def __init__(self):
    # Initialiser le capteur AHT20
        i2c = board.I2C()
        self.sensor = adafruit_ahtx0.AHTx0(i2c)
    # Initialiser le client MQTT
        self.client = MQTTClient(ADAFRUIT_IO_USERNAME, ADAFRUIT_IO_KEY)
        self.client.on_connect = connected
        self.client.on_disconnect = disconnected
    
    def connect(self):
        """Se connecter a Adafruit IO."""
        print("Connexion a Adafruit IO...")
        self.client.connect()
        self.client.loop_background()
    
    def read_and_publish(self):
        """Lire les capteurs et publier vers les feeds."""
        # Lire les valeurs
        temperature = round(self.sensor.temperature, 1)
        humidity = round(self.sensor.relative_humidity, 1)
        print(f"Temperature: {temperature}C, Humidite: {humidity} %")
        
        
        self.client.publish('temperature', temperature)
        print(f" -> temperature: {temperature}")
        time.sleep(3) # Respecter le rate limit
        self.client.publish('humidity', humidity)
        print(f" -> humidity: {humidity}")
        time.sleep(3)

def main():
    PUBLISH_INTERVAL = 3
    publisher = SensorPublisher()
    publisher.connect()

    print(f"Publication toutes les {PUBLISH_INTERVAL} secondes...")
    print("Appuyez sur Ctrl+C pour arreter")
    print("-" * 40)
    try:
        while True:
            publisher.read_and_publish()
            time.sleep(PUBLISH_INTERVAL)
    except KeyboardInterrupt:
        print("\nArret demande par l'utilisateur")

if __name__ == "__main__":
    main()