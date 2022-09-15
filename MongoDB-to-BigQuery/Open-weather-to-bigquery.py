from google.cloud import bigquery
from google.cloud import pubsub_v1
import os
import json
from pymongo import MongoClient
from flask import Flask, render_template
from datetime import datetime


app = Flask(__name__)

#credentials and for google srvice account
credentials_path="heidelberg-wt-visualization-d143241a0def.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=credentials_path

# credentials for pubsub and topic variable define
publisher = pubsub_v1.PublisherClient()
topic_path = 'projects/heidelberg-wt-visualization/topics/topicweather'

@app.route('/',methods=["GET","POST"])
def mongo_to_pubsub():

    service = os.environ.get('K_SERVICE', 'Unknown service')
    revision = os.environ.get('K_REVISION', 'Unknown revision')

    #BigQuery Query
    client = bigquery.Client()
    query_job_bigQuery = client.query("""SELECT MAX(_id) FROM heidelberg-wt-visualization.myproject.openweathertable;""")
    results = query_job_bigQuery.result()
    last_id=results.__next__()[0]
    message="Message Published Successfully"

    #credentials and cluster infor from Mongo db
    cluster=MongoClient("mongodb+srv://Sasha:9eeJEgyGYkZPc6VY@cluster0.i2q1kde.mongodb.net/?retryWrites=true&w=majority")
    db=cluster["API_data"]
    collection=db["weather"]
    query_job_mongoDB={"_id":{"$gt":last_id}}
    results=collection.find(query_job_mongoDB)

    for result in results:
        _id=result['_id']
        date=datetime.fromtimestamp(result['_id']).date()
        hour=datetime.fromtimestamp(result['_id']).hour
        sunrise=result['sunrise']
        sunset=result['sunset']
        temp=result['temp']
        feels_like=result['feels_like']
        pressure=result['pressure']
        humidity=result['humidity']
        dew_point=result['dew_point']
        uvi=result['uvi']
        clouds=result['clouds']
        visibility=result['visibility']
        wind_speed=result['wind_speed']
        wind_deg=result['wind_deg']
        try:
            rain=result['rain']["1h"]
        except:
            rain=0

        message_json = json.dumps({
            "_id":_id,
            "date":date,
            "hour":hour,
            "sunrise": sunrise,
            "sunset": sunset,
            "temp": temp,
            "feels_like": feels_like,
            "pressure": pressure,
            "humidity": humidity,
            "dew_point": dew_point,
            "uvi": uvi,
            "clouds": clouds,
            "visibility": visibility,
            "wind_speed": wind_speed,
            "wind_deg": wind_deg,
            "rain":rain
             },default=str)

        message_bytes = message_json.encode('utf-8')

        try:
            publish_future = publisher.publish(topic_path, data=message_bytes)
            publish_future.result()
        except Exception as e:
            print(e)

    return render_template('index.html',
                            message=message,
                            Service=service,
                            Revision=revision)

if __name__ == '__main__':
    server_port = os.environ.get('PORT', '8081')
    app.run(debug=False, port=server_port, host='0.0.0.0')
