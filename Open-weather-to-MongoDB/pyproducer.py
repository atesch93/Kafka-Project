
import os
import requests
from kafka.producer import KafkaProducer
from flask import Flask, render_template, request

API_key = ''
lat = 49.414212
lon = 8.709540
# pylint: disable=C0103
app = Flask(__name__)


@app.route('/', methods=["GET", "POST"])
def pyproducer():
    message = "No Post Request Recieved!"
    """Get Cloud Run environment variables."""
    service = os.environ.get('K_SERVICE', 'Unknown service')
    revision = os.environ.get('K_REVISION', 'Unknown revision')

    url = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={API_key}"
    payload = {}

    producer = KafkaProducer(bootstrap_servers="kafka-83d2d76-stud-9045.aivencloud.com:15768",
                             security_protocol="SSL",
                             ssl_cafile="ca.pem",
                             ssl_certfile="service.cert",
                             ssl_keyfile="service.key"
                             )

    response = requests.request("GET", url, data=payload)
    producer.send(topic="weather", key=f"id".encode('utf-8'), value=response.text.encode('utf-8'))

    return render_template('index.html',
                           message=message,
                           Service=service,
                           Revision=revision)


if __name__ == '__main__':
    server_port = os.environ.get('PORT', '8080')
    app.run(debug=False, port=server_port, host='0.0.0.0')
