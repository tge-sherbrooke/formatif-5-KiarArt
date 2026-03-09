# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "adafruit-io",
#   "rpi.gpio",
# ]
# ///
"""
Test de publication MQTT vers Adafruit IO
Cours 243-413-SH, Semaine 5
"""

from Adafruit_IO import MQTTClient
import time

# Importer la configuration (username et cle API)
from config import ADAFRUIT_IO_USERNAME, ADAFRUIT_IO_KEY

# Callbacks MQTT
def connected(client):
    """Callback quand connecte au broker."""
    print("Connecte a Adafruit IO!")

def disconnected(client):
    """Callback quand deconnecte du broker."""
    print("Deconnecte de Adafruit IO")

def message(client, feed_id, payload):
    """Callback quand un message est recu."""
    print(f"Recu sur {feed_id}: {payload}")

def main():
    # Creer le client MQTT
    client = MQTTClient(ADAFRUIT_IO_USERNAME, ADAFRUIT_IO_KEY)
    # Assigner les callbacks
    client.on_connect = connected
    client.on_disconnect = disconnected
    client.on_message = message
    
    # Se connecter

    print("Connexion a Adafruit IO...")
    client.connect()

    # Demarrer la boucle MQTT en arriere-plan
    client.loop_background()

 # Publier une valeur de test
    print("Publication vers le feed 'temperature'...")
    client.publish('temperature', 22.5)
    print("Valeur 22.5 publiee!")

 # Attendre un peu pour voir le resultat
    time.sleep(3)

    print("Test termine. Verifiez sur Adafruit IO!")

if __name__ == "__main__":
    main()