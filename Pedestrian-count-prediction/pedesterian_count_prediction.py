from google.cloud import bigquery
import os
import json
from pymongo import MongoClient
from flask import Flask, render_template
from datetime import datetime
import pandas as pd
import pickle
from sklearn.ensemble import RandomForestRegressor
from joblib import dump, load

app = Flask(__name__)

# credentials and for google srvice account
credentials_path = "heidelberg-wt-visualization-d143241a0def.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path


@app.route('/', methods=["GET", "POST"])
def prediction():
    service = os.environ.get('K_SERVICE', 'Unknown service')
    revision = os.environ.get('K_REVISION', 'Unknown revision')
    message = "Forecasts are updated now in hourly_forecast table"

    client = bigquery.Client()
    query_job_bigQuery = client.query("""SELECT * FROM heidelberg-wt-visualization.myproject.hourly_forecast;""")
    results = query_job_bigQuery.result()
    loaded_model = load('rf_regressor.joblib')

    for row in results:
        X = [row[1], row[2], row[3], row[4], row[5]]
        prediction = round((loaded_model.predict([X])[0]))
        client.query(
            f"UPDATE heidelberg-wt-visualization.myproject.hourly_forecast SET pedestrian_prediction={prediction} WHERE _id={row[0]};")
    return render_template('index.html',
                           message=message,
                           Service=service,
                           Revision=revision)


if __name__ == '__main__':
    server_port = os.environ.get('PORT', '8080')
    app.run(debug=False, port=server_port, host='0.0.0.0')