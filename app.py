from flask import Flask, request
from waitress import serve
import pymongo
import json
import sys

app = Flask(__name__)


def is_true(val):
    return len(val) > 0 and val.lower() == "true" or val == "1"



@app.before_request
def fix_transfer_encoding():
    
    transfer_encoding = request.headers.get("Transfer-Encoding", None)
    if transfer_encoding == u"chunked":
        request.environ["wsgi.input_terminated"] = True

@app.route("/")
def hello_world():
    return "<p>This is a Hello World application</p>"


@app.route("/temperature", defaults={"path": "" }, methods = ["GET"])
def temprange(path):

    conn = pymongo.MongoClient('mongodb://mongo:27017')
    print("ESTABLISHED CONNECTION TO DATABASE")
    db = conn["test"]
    experiments_records = db["records"]
    experiments_config = db["experiments"]

    payload = []

    ID = request.args.get("experiment-id")
    StartTime = float(request.args["start-time"])
    EndTime = float(request.args["end-time"])

    for rec in experiments_records.find({"exp_id": ID}):
        if (rec["timestamp"] >= StartTime and rec["timestamp"] <= EndTime): #maybe < and > instead
                payload.append({
                    "timestamp": rec["timestamp"],
                    "temperature": rec["avg_temperature"]
                })

    return(json.dumps(payload))

@app.route("/temperature/out-of-range", methods=["GET"])
def tempoor():

    conn = pymongo.MongoClient('mongodb://mongo:27017')
    print("ESTABLISHED CONNECTION TO DATABASE", file=sys.stderr)
    db = conn["test"]
    experiments_records = db["records"]
    experiments_config = db["experiments"]

    ID = request.args.get("experiment-id")

    payload = []
    lowertemp = None
    uppertemp = None
    for rec in experiments_config.find({"exp_id": ID}):
            uppertemp = rec["temp_max_threshold"]
            lowertemp = rec["temp_min_threshold"]
            print("experimentID found", file=sys.stderr)
            print(uppertemp, file=sys.stderr)
            print(lowertemp, file=sys.stderr)

    if lowertemp == None and uppertemp == None:
        return "No experiment found"

    for rec in experiments_records.find({"exp_id": ID, "avg_temperature" : {"$lt" : lowertemp}}):
            payload.append({
                "timestamp": rec["timestamp"],
                "temperature": rec["avg_temperature"]
            })

    for rec in experiments_records.find({"exp_id": ID, "avg_temperature" : {"$gt" : uppertemp}}):
            payload.append({
                "timestamp": rec["timestamp"],
                "temperature": rec["avg_temperature"]
            })

    return(json.dumps(payload))

if __name__ == '__main__':
    serve(app, host='0.0.0.0', port=3003)