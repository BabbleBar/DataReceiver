import json
import os

import pika
import xmltodict
from flask import Flask, request

app = Flask(__name__)


@app.route("/ping")
def hello():
    return "pong"


def get_pika_params():
    if 'VCAP_SERVICES' in os.environ:
        vcap_services = json.loads(os.environ['VCAP_SERVICES'])

        return pika.URLParameters(url=vcap_services['rabbitmq'][0]['credentials']['uri'])

    return pika.ConnectionParameters(host="localhost")


@app.route("/cb", methods=["POST"])
def cb():
    LrnDevEui = request.args.get('LrnDevEui', '')
    LrnFPort = request.args.get('LrnFPort', '')
    LrnInfos = request.args.get('LrnInfos', '')

    print("New Infos -- DevEui: %s / FPort: %s / Infos: %s" % (LrnDevEui, LrnFPort, LrnInfos))
    data = request.get_data(as_text=True)
    dict_data = xmltodict.parse(data)
    json_data = json.dumps(dict_data['DevEUI_uplink'], indent=4, separators=(',', ': '))
    print(json_data)

    channel.basic_publish(exchange='data_log',
                          routing_key='',
                          body=json_data)

    return "processed"


if __name__ == "__main__":
    port = os.getenv('VCAP_APP_PORT', '5000')

    connection = pika.BlockingConnection(get_pika_params())
    channel = connection.channel()
    channel.exchange_declare(exchange='data_log', type='fanout')
    app.run(host='0.0.0.0', port=int(port))
