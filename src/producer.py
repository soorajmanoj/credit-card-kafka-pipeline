from utils import DataProcessor
import time
import datetime
import decimal


class KafkaTransactionProducer:
    def __init__(self):
        self.dp = DataProcessor()
        self.conn = self.dp.get_mysql_connection()
        self.cursor = self.conn.cursor(dictionary=True)
        self.producer = self.dp.get_kafka_producer()
        self.topic = "transactions"  # Kafka topic name

    def stream_transactions(self):
        # Read transactions from MySQL
        self.cursor.execute("SELECT * FROM transactions ORDER BY timestamp")
        rows = self.cursor.fetchall()
        # ensure time-sequenced

        print(f"Streaming {len(rows)} transactions to Kafka...")

        for record in rows:
            # 🛠 Fix datetime serialization
            for key, value in record.items():
                if isinstance(value, datetime.datetime):
                    record[key] = value.isoformat()
                elif isinstance(value, decimal.Decimal):
                    record[key] = float(value)

            self.producer.send(self.topic, value=record)
            # time.sleep(0.1)  # simulate real-time streaming

        self.producer.flush()
        print("All transactions sent.")


if __name__ == "__main__":
    producer = KafkaTransactionProducer()
    producer.stream_transactions()
