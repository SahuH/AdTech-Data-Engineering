from kafka import KafkaProducer
import json
import csv
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from io import BytesIO

# Read JSON file and produce messages to Kafka
def ingest_json(producer, json_file_path, kafka_topic):
    with open(json_file_path, 'r') as file:
        for line in file:
            message = json.loads(line)
            producer.send(kafka_topic, value=json.dumps(message).encode('utf-8'))

# Read CSV file and produce messages to Kafka
def ingest_csv(producer, csv_file_path, kafka_topic):
    with open(csv_file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            message = json.dumps(row)
            producer.send(kafka_topic, value=message.encode('utf-8'))

# Read Avro file and produce messages to Kafka
def ingest_avro(producer, avro_file_path, kafka_topic):
    with open(avro_file_path, 'rb') as file:
        reader = DataFileReader(file, DatumReader())
        for record in reader:
            message = json.dumps(record)
            producer.send(kafka_topic, value=message.encode('utf-8'))


producer = KafkaProducer(bootstrap_servers='localhost:9092')

ingest_json(producer, 'ad_impressions.json', 'ad_impressions')
ingest_csv(producer, 'clicks_conversions.csv', 'clicks_conversions')
ingest_avro(producer, 'bid_requests.avro', 'bid_requests')

producer.close()
