from kafka import KafkaProducer
import json
import time
import random

# Conexión a Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

while True:
    # Generar dato de prueba
    data = {"origen": "productor1", "valor": random.randint(10, 50)}
    producer.send('topic_datos', value=data)
    print(f"Enviado desde P1: {data}")
    time.sleep(2) # Envía cada 2 segundos