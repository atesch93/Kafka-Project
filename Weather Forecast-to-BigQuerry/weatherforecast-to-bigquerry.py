"""
A sample Hello World server.
"""
import os
import requests
import json

from google.cloud import bigquery

from flask import Flask, render_template

credentials_path = "heidelberg-wt-visualization-d143241a0def.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# pylint: disable=C0103
app = Flask(__name__)


@app.route('/', methods=["GET", "POST"])
def api_call():
    """Return a friendly HTTP greeting."""
    message = "It's running!"

    response = requests.get(
        "https://api.openweathermap.org/data/2.5/onecall?lat=49.40768&lon=8.69079&appid=9001d691d1fb852e715b5524d9860ecc")
    data = response.json()["hourly"]

    """Get Cloud Run environment variables."""
    service = os.environ.get('K_SERVICE', 'Unknown service')
    revision = os.environ.get('K_REVISION', 'Unknown revision')

    rows = []

    for i in data:
        try:
            rain = i["rain"]["1h"]
        except:
            rain = 0
        message_dict = {
            "_id": i["dt"],
            "temp": i["temp"],
            "humidity": i["humidity"],
            "clouds": i["clouds"],
            "wind_speed": i["wind_speed"],
            "rain": rain,
            "pedestrian_prediction": 0}
        rows.append(message_dict)

    client = bigquery.Client()
    table_ref = client.dataset('myproject').table('hourly_forecast')
    table = client.get_table(table_ref)
    client.insert_rows(table, rows)

    return render_template('index.html',
                           message=message,
                           Service=service,
                           Revision=revision)


if __name__ == '__main__':
    server_port = os.environ.get('PORT', '8080')
    app.run(debug=False, port=server_port, host='0.0.0.0')
