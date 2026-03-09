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
Publication MQTT robuste avec reconnexion et buffering
Cours 243-413-SH, Semaine 5
"""

from Adafruit_IO import MQTTClient
import board
import adafruit_ahtx0
import time
import random
from config import ADAFRUIT_IO_USERNAME, ADAFRUIT_IO_KEY, PUBLISH_INTERVAL

class MQTTReconnector:
 """Gere la reconnexion MQTT avec backoff exponentiel."""

 MIN_DELAY = 1  # Delai initial 
 MAX_DELAY = 120    # Delai maximum
 JITTER = 0.25  # Variation aleatoire: +/- 25%

 def __init__(self, client):
    self.client = client
    self.delay = self.MIN_DELAY
    self.buffer = [] # Buffer pour les donnees pendant deconnexion
    self.connected = False

 def on_connect(self, client):
    """Callback de connexion."""
    print("Connecte a Adafruit IO!")
    self.connected = True
    self.delay = self.MIN_DELAY # Reset du delai
    self._flush_buffer()

 def on_disconnect(self, client):
    """Callback de deconnexion."""
    print("Deconnecte de Adafruit IO")
    self.connected = False
    self.reconnect()

 def reconnect(self):
    """Tentative de reconnexion avec backoff exponentiel."""
    while not self.connected:
        try:
            print(f"Tentative de reconnexion...")
            self.client.connect()
            self.client.loop_background()
            return # Si succes
        except Exception as e:
            # Calculer le delai avec jitter
            jitter = self.delay * self.JITTER * (random.random() * 2 - 1)
            actual_delay = self.delay + jitter
            print(f"Echec. Prochaine tentative dans {actual_delay:.1f}s...")
            time.sleep(actual_delay)
            
            self.delay = min(self.delay * 2, self.MAX_DELAY)

 def buffer_data(self, feed, value):
    """Ajoute des donnees au buffer pendant la deconnexion."""

    self.buffer.append((feed, value, time.time()))
    print(f" [Buffer] {feed}: {value} (total: {len(self.buffer)})")

    if len(self.buffer) > 100:
        self.buffer.pop(0) # Supprimer plus ancien

 def _flush_buffer(self):
    """Envoie les donnees bufferisees apres reconnexion."""
    if not self.buffer:
        return

    print(f"Envoi de {len(self.buffer)} messages bufferises...")

    while self.buffer and self.connected:
        feed, value, timestamp = self.buffer.pop(0)
        age = time.time() - timestamp

        if age > 3600:
            print(f" [Skip] {feed}: {value} (trop ancien: {age:.0f}s)")
            continue

        try:
            self.client.publish(feed, value)
            print(f" [Sent] {feed}: {value}")
            time.sleep(3) 
        except Exception as e:
            
            self.buffer.insert(0, (feed, value, timestamp))
            print(f" [Fail] Remis dans le buffer")
            break


class RobustSensorPublisher:
   """Publication robuste des capteurs avec gestion des erreurs."""
   def __init__(self):
   # Initialis le capteur
       i2c = board.I2C()
       self.sensor = adafruit_ahtx0.AHTx0(i2c)

       # Initialis client MQTT
       self.client = MQTTClient(ADAFRUIT_IO_USERNAME, ADAFRUIT_IO_KEY)

       # Initialise gestionnaire de reconnexion
       self.reconnector = MQTTReconnector(self.client)

       # Assigne callbacks
       self.client.on_connect = self.reconnector.on_connect
       self.client.on_disconnect = self.reconnector.on_disconnect
       

   def connect(self):
       """Connexion initiale."""
       print("Connexion initiale a Adafruit IO...")
       self.client.connect()
       self.client.loop_background()
       self.reconnector.connected = True

   def publish_safe(self, feed, value):
       """Publie une valeur, bufferise si deconnecte."""
       if self.reconnector.connected:
           try:
               self.client.publish(feed, value)
               print(f" -> {feed}: {value}")

           except Exception as e:
               print(f" [Error] {feed}: {e}")
               self.reconnector.buffer_data(feed, value)

       else:
           self.reconnector.buffer_data(feed, value)

   def read_and_publish(self):
       """Lire les capteurs et publier de maniere robuste."""
       try:
           temperature = round(self.sensor.temperature, 1)
           humidity = round(self.sensor.relative_humidity, 1)
           print(f"Lecture: {temperature}C, {humidity} %")

           self.publish_safe('temperature', temperature)
           time.sleep(3)

           self.publish_safe('humidity', humidity)
           time.sleep(3)

       except Exception as e:
           print(f"Erreur lecture capteur: {e}")

def main():
   publisher = RobustSensorPublisher()
   publisher.connect()

   print(f"Publication robuste toutes les {PUBLISH_INTERVAL} secondes...")
   print("Appuyez sur Ctrl+C pour arreter")
   print("-" * 50)

   try:
       while True:
           publisher.read_and_publish()
           time.sleep(PUBLISH_INTERVAL)

   except KeyboardInterrupt:
       print("\nArret demande par l'utilisateur")

if __name__ == "__main__":
    main()