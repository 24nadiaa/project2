import pika
import mysql.connector
import json
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='task_queue')
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="8534",
    database="tempmon"
)
cursor = conn.cursor()
query = "SELECT device_oid, temperature FROM device_temperatures"
cursor.execute(query)
devices = cursor.fetchall()
for device_oid, temperature in devices:
    
    data = {
        'device_oid': device_oid,
        'temperature': temperature
    }
    channel.basic_publish(exchange='', routing_key='task_queue', body=json.dumps(data))
conn.close()
connection.close()