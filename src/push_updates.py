import csv
from utils import DataProcessor


class UpdatePusher:
    def __init__(self):
        self.dp = DataProcessor()
        self.conn = self.dp.get_mysql_connection()
        self.cursor = self.conn.cursor()

    def update_cards(self, path="results/cards_updated.csv"):
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.cursor.execute(
                    """
                    UPDATE cards
                    SET credit_limit = %s,
                        current_balance = %s
                    WHERE card_id = %s
                """,
                    (
                        float(row["credit_limit"]),
                        float(row["current_balance"]),
                        int(row["card_id"]),
                    ),
                )
        self.conn.commit()
        print("cards table updated")

    def update_customers(self, path="results/customers_updated.csv"):
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.cursor.execute(
                    """
                    UPDATE customers
                    SET credit_score = %s,
                        annual_income = %s
                    WHERE customer_id = %s
                """,
                    (
                        float(row["credit_score"]),
                        float(row["annual_income"]),
                        int(row["customer_id"]),
                    ),
                )
        self.conn.commit()
        print("customers table updated")

    def insert_stream_transactions(self, path="results/stream_transactions.csv"):
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            self.cursor.execute("DELETE FROM stream_transactions")
            for row in reader:
                self.cursor.execute(
                    """
                    INSERT INTO stream_transactions (
                        transaction_id, card_id, merchant_name, timestamp,
                        amount, location, transaction_type, related_transaction_id,
                        status, pending_balance
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        int(row["transaction_id"]),
                        int(row["card_id"]),
                        row["merchant_name"],
                        row["timestamp"],
                        float(row["amount"]),
                        row["location"],
                        row["transaction_type"],
                        (
                            int(row["related_transaction_id"])
                            if row["related_transaction_id"]
                            else None
                        ),
                        row["status"],
                        float(row["pending_balance"]),
                    ),
                )
        self.conn.commit()
        print("stream_transactions table updated")

    def insert_batch_transactions(self, path="results/batch_transactions.csv"):
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            self.cursor.execute("DELETE FROM batch_transactions")
            for row in reader:
                self.cursor.execute(
                    """
                    INSERT INTO batch_transactions (
                        transaction_id, card_id, merchant_name, timestamp,
                        amount, location, transaction_type, related_transaction_id,
                        status, pending_balance
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        int(row["transaction_id"]),
                        int(row["card_id"]),
                        row["merchant_name"],
                        row["timestamp"],
                        float(row["amount"]),
                        row["location"],
                        row["transaction_type"],
                        (
                            int(row["related_transaction_id"])
                            if row["related_transaction_id"]
                            else None
                        ),
                        row["status"],
                        float(row["pending_balance"]),
                    ),
                )
        self.conn.commit()
        print("batch_transactions table updated")

    def close(self):
        self.cursor.close()
        self.conn.close()

    def run(self):
        print("Pushing all updates to MySQL...")
        self.update_cards()
        self.update_customers()
        self.insert_stream_transactions()
        self.insert_batch_transactions()
        self.close()
        print("All tables updated successfully.")


if __name__ == "__main__":
    updater = UpdatePusher()
    updater.run()
