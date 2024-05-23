import pika
import json
from influxdb import InfluxDBClient
influx_client = InfluxDBClient(host='localhost', port=8086, username='admin', password='password', database='temperature')

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='task_queue')
def callback(ch, method, properties, body):
    data = json.loads(body)
    device_oid = data['device_oid']
    temperature = data['temperature']
    json_body = [
        {
            "measurement": "temperature",
            "tags": {
                "device_oid": device_oid
            },
            "fields": {
                "temperature": float(temperature)
            }
        }
    ]
    influx_client.write_points(json_body)
    print(f"Temperature data collected and stored for device OID: {device_oid}")
channel.basic_consume(queue='task_queue', on_message_callback=callback, auto_ack=True)
print('Waiting for messages...')
channel.start_consuming()