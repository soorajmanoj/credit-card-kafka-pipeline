from dotenv import load_dotenv
import os
from kafka import KafkaProducer
import mysql.connector
import json


class DataProcessor:
    def __init__(self):
        # Load environment variables
        load_dotenv()

        # Database Config
        self.mysql_host = os.getenv("MYSQL_HOST")
        self.mysql_port = os.getenv("MYSQL_PORT")
        self.mysql_db = os.getenv("MYSQL_DB", "credit_system")
        self.mysql_user = os.getenv("MYSQL_USER")
        self.mysql_password = os.getenv("MYSQL_PASSWORD")
        self.mysql_connector_path = os.getenv("MYSQL_CONNECTOR_PATH")

        # Kafka Config
        self.kafka_broker = os.getenv("KAFKA_BROKER")

        # Session placeholders
        self.producer = None
        self.mysql_conn = None

    def get_jdbc_url(self):
        return f"jdbc:mysql://{self.mysql_host}:{self.mysql_port}/{self.mysql_db}"

    def get_mysql_properties(self):
        return {
            "user": self.mysql_user,
            "password": self.mysql_password,
            "driver": "com.mysql.cj.jdbc.Driver",
        }

    def get_mysql_connection(self):
        if not self.mysql_conn:
            self.mysql_conn = mysql.connector.connect(
                host=self.mysql_host,
                user=self.mysql_user,
                password=self.mysql_password,
                database=self.mysql_db,
            )
        return self.mysql_conn

    def get_kafka_producer(self):
        if not self.producer:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        return self.producer
