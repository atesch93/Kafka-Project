from google.cloud import bigquery
import os
import json
from pymongo import MongoClient
from flask import Flask, render_template
from datetime import datetime


app = Flask(__name__)

#credentials and for google srvice account
credentials_path="heidelberg-wt-visualization-d143241a0def.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=credentials_path

@app.route('/',methods=["GET","POST"])
def mongo_to_pubsub():

    service = os.environ.get('K_SERVICE', 'Unknown service')
    revision = os.environ.get('K_REVISION', 'Unknown revision')

    #BigQuery Query
    client = bigquery.Client()
    query_job_bigQuery = client.query("""SELECT MAX(_id) FROM heidelberg-wt-visualization.myproject.pedestrian;""")
    results = query_job_bigQuery.result()
    last_id=results.__next__()[0]
    message="Message Published Successfully"

    #credentials and cluster infor from Mongo db
    cluster=MongoClient("mongodb+srv://Sasha:9eeJEgyGYkZPc6VY@cluster0.i2q1kde.mongodb.net/?retryWrites=true&w=majority")
    db=cluster["API_data"]
    collection=db["Heidelberg_Pedestrian"]
    query_job_mongoDB={"key":{"$gt":last_id}}
    results=collection.find(query_job_mongoDB)
    json_rows = []

    for result in results:
        _id=int(result['key'])
        date=datetime.fromtimestamp(_id).date()
        hour=datetime.fromtimestamp(_id).hour
        location=result['location']
        city=result['city']
        time=result['time']
        weekday=result['weekday']
        pedestrians=int(result['pedestrians_count'])
        temperature=int(result['temperature'])
        condition=result['condition']


        message_dict ={
            "_id":_id,
            "date":str(date),
            "hour":hour,
            "location": location,
            "city": city,
            "time": time,
            "weekday": weekday,
            "pedestrians": pedestrians,
            "temperature": temperature,
            "condition": condition
             }

        json_rows.append(message_dict)

    try:
        client.insert_rows_json("heidelberg-wt-visualization.myproject.pedestrian", json_rows)
    except:
        print("no update in pedestrian table")
    return render_template('index.html',
                            message=message,
                            Service=service,
                            Revision=revision)
if __name__ == '__main__':
    server_port = os.environ.get('PORT', '8080')
    app.run(debug=False, port=server_port, host='0.0.0.0')

