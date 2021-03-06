import json
import os
import sys
import traceback

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


def parse_value(data, data_type):
    return {
        'eui': data['DevEUI'],
        'data_type': data_type,
        'timestamp': data['Time'],
        'payload_hex': data['payload_hex'],
        'payload_int': int(data['payload_hex'], 16),
        'lat': float(data['LrrLAT']),
        'lon': float(data['LrrLON']),
    }


def send_value(data, data_type):
    dict_data = parse_value(data, data_type)
    json_data = json.dumps(dict_data, indent=4, separators=(',', ': '))
    print("SendVALUE: ")
    print(json_data)
    channel_data.basic_publish(exchange='data',
                               routing_key='',
                               body=json_data)


@app.route("/cb", methods=["POST"])
def cb():
    LrnDevEui = request.args.get('LrnDevEui', '')
    LrnFPort = request.args.get('LrnFPort', '')
    LrnInfos = request.args.get('LrnInfos', '')

    print("New Infos -- DevEui: %s / FPort: %s / Infos: %s" % (LrnDevEui, LrnFPort, LrnInfos))
    data = request.get_data(as_text=True)
    dict_data = xmltodict.parse(data)['DevEUI_uplink']
    json_data = json.dumps(dict_data, indent=4, separators=(',', ': '))

    channel_log.basic_publish(exchange='data_log',
                              routing_key='',
                              body=json_data)
    try:
        if LrnFPort == '3':
            send_value(dict_data, 'lum')
        elif LrnFPort == '4':
            send_value(dict_data, 'spl')
        else:
            print("Unknown port: %s" % LrnFPort)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  limit=2, file=sys.stdout)
        raise

    return "processed"


if __name__ == "__main__":
    port = os.getenv('VCAP_APP_PORT', '5000')

    connection = pika.BlockingConnection(get_pika_params())
    channel_log = connection.channel()
    channel_log.exchange_declare(exchange='data_log', type='fanout')
    channel_data = connection.channel()
    channel_data.exchange_declare(exchange='data', type='fanout')
    app.run(host='0.0.0.0', port=int(port))
