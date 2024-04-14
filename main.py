import json
import network
import random
import rp2
import time
import urequests

from umqtt.simple import MQTTClient
from boot import *

MQTT_SERVER = "broker.emqx.io" #54.146.113.169
MQTT_PORT = 1883
MQTT_CLIENT_ID = "micropython-client-{id}".format(id = random.getrandbits(8))
MQTT_USERNAME = "emqx"
MQTT_PASSWORD = "public"
MQTT_SUB_TOPIC = "websummit/offload"
MQTT_PUB_TOPIC = "websummit/ping"


def connect_wifi():
    rp2.country('BR')
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    # Comes from `boot`
    wlan.connect(WIFI_SSID, WIFI_PASSWORD)
    print ("Connecting ...")
    while not wlan.isconnected() and wlan.status() >= 0:
        time.sleep(1)
    if wlan.isconnected():
        print("Connected with IP: " + wlan.ifconfig()[0])


def connect_mqtt():
    client = MQTTClient(
        MQTT_CLIENT_ID, MQTT_SERVER, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD)
    client.connect()
    print('Connected to MQTT Broker "{server}"'.format(server = MQTT_SERVER))
    return client


def get_star_wars(movie=1):
    url = "https://swapi.dev/api/films/{0}/".format(movie)
    print("Fetching {0}".format(url))
    r = urequests.get(url)
    if r.status_code == 200:
        data = json.loads(r.text)
    else:
        print('Error: {0}'.format(r.status_code))
    r.close()
    return data


def on_message(topic, msg):
    topic = topic.decode()
    payload = json.loads(msg.decode())
    print("Received '{0}' from topic '{1}'".format(payload, topic))

    d = get_star_wars(movie=payload.get("movie"))
    print('Episode {0}: {1}\n{2}'.format(d['episode_id'], d['title'], d['opening_crawl']))

    #
    # XXX use `presigned url` to save data direclty to OBJS
    #


def publish_loop(client):
    client.set_callback(on_message)
    client.subscribe(MQTT_SUB_TOPIC)
    msg_count = 0
    while True:
        msg_dict = {"msg": "All clear!", "count": msg_count}
        msg = json.dumps(msg_dict)
        result = client.publish(MQTT_PUB_TOPIC, msg)
        print("Send '{msg}' to topic '{topic}'".format(msg = msg, topic = MQTT_PUB_TOPIC))
        client.check_msg()
        msg_count += 1
        time.sleep(5)


def main():
    connect_wifi()
    client = connect_mqtt()
    publish_loop(client)


if __name__ == "__main__":
    main()
