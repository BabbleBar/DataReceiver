from flask import Flask, request
import os
import sys
import pika
import json

app = Flask(__name__)


@app.route("/")
def hello():
    return "Hello World!"


def get_pika_params():
    if 'VCAP_SERVICES' in os.environ:
        VCAP_SERVICES = json.loads(os.environ['VCAP_SERVICES'])

        return pika.URLParameters(url=VCAP_SERVICES['rabbitmq'][0]['credentials']['uri'])

    return pika.ConnectionParameters(host="localhost")


@app.route("/cb", methods=["POST"])
def cb():
    LrnDevEui = request.args.get('LrnDevEui', '')
    LrnFPort = request.args.get('LrnFPort', '')
    LrnInfos = request.args.get('LrnInfos', '')

    print("New Infos -- DevEui: %s / FPort: %s / Infos: %s" % (LrnDevEui, LrnFPort, LrnInfos))
    data = request.get_data(as_text=True)
    print(data)

    channel.basic_publish(exchange='data_log',
                          routing_key='',
                          body=data)

    return "processed"


port = os.getenv('VCAP_APP_PORT', '5000')

if __name__ == "__main__":
    connection = pika.BlockingConnection(get_pika_params())
    channel = connection.channel()
    channel.exchange_declare(exchange='data_log', type='fanout')
    app.run(host='0.0.0.0', port=int(port), debug=True)
