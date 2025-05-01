import json
import os
import csv
import re
from kafka import KafkaConsumer
from utils import DataProcessor


class KafkaTransactionConsumerSimpleReal:
    def __init__(self):
        self.dp = DataProcessor()
        self.consumer = KafkaConsumer(
            "transactions",
            bootstrap_servers=self.dp.kafka_broker,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="transaction-consumers",
            consumer_timeout_ms=5000,
        )
        self.conn = self.dp.get_mysql_connection()
        self.cursor = self.conn.cursor(dictionary=True)
        self.card_pending_balances = {}
        self.output_records = []

    def get_card_info(self, card_id):
        self.cursor.execute(
            """
            SELECT customer_id, credit_limit, current_balance 
            FROM cards WHERE card_id = %s
        """,
            (card_id,),
        )
        return self.cursor.fetchone()

    def get_customer_zip(self, customer_id):
        self.cursor.execute(
            """
            SELECT address FROM customers WHERE customer_id = %s
        """,
            (customer_id,),
        )
        row = self.cursor.fetchone()
        if row and row["address"]:
            match = re.search(r"\b\d{5}\b", row["address"])
            if match:
                return match.group(0)
        return None

    def extract_zip(self, text):
        if not text:
            return None
        match = re.search(r"\b\d{5}\b", text)
        if match:
            return match.group(0)
        return None

    def is_location_close_enough(self, zip1, zip2):
        """Determine if merchant location is close enough to the customer's address.

        Returns:
            bool: True if the locations are close enough to approve, False if too far apart
        """
        if not zip1 or not zip2 or len(zip1) < 5 or len(zip2) < 5:
            # Can't determine distance with invalid zips
            return False  # Safer to reject when we can't verify

        # Check first digits of zip codes to determine proximity
        # Increased allowed distance by allowing first digit to be different
        if zip1[:1] != zip2[:1]:
            # Different first digit - very far apart (different regions)
            return False

        if zip1[:2] != zip2[:2]:
            # Different second digit but same first digit
            # Moderately far but will now approve these
            return True

        # Same first 2+ digits - close enough
        return True

    def validate_transaction(self, record):
        card_id = int(record["card_id"])
        card_info = self.get_card_info(record["card_id"])
        if not card_info:
            print(f"Card ID {record['card_id']} not found.")
            return

        customer_id = card_info["customer_id"]
        card_limit = float(card_info["credit_limit"])
        pending_balance = self.card_pending_balances.get(
            card_id, float(card_info["current_balance"])
        )
        card_id = int(record["card_id"])
        pending_balance = self.card_pending_balances.get(
            card_id, float(card_info["current_balance"])
        )
        amount = float(record.get("amount", 0))

        customer_zip = self.get_customer_zip(customer_id)
        merchant_zip = self.extract_zip(record.get("location"))

        declined = False
        reason = ""

        txn_type = record.get("transaction_type", "").lower()

        if txn_type in ["refund", "cancellation"]:
            declined = False  # Always accept refunds/cancellations
        else:
            if amount >= 0.5 * card_limit:
                declined = True
                reason = "Amount >= 50% of credit limit"
            elif (pending_balance + amount) > card_limit:
                declined = True
                reason = "Pending + amount exceeds credit limit"
            elif txn_type == "purchase" and not self.is_location_close_enough(
                customer_zip, merchant_zip
            ):
                declined = True
                reason = "Merchant too far from customer"

        if declined:
            record["validated_status"] = "declined"
            record["pending_balance"] = round(pending_balance, 2)
            print(f"Declined transaction {record['transaction_id']} — {reason}")
        else:
            record["validated_status"] = "pending"
            pending_balance += amount
            self.card_pending_balances[card_id] = pending_balance
            record["pending_balance"] = round(pending_balance, 2)

        self.output_records.append(record)

    def start(self):
        print("Listening for transactions...")
        for message in self.consumer:
            record = message.value
            self.validate_transaction(record)

        # Save results
        if not os.path.exists("results"):
            os.makedirs("results")

        with open("results/stream_transactions.csv", mode="w", newline="") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=[
                    "transaction_id",
                    "card_id",
                    "merchant_name",
                    "timestamp",
                    "amount",
                    "location",
                    "transaction_type",
                    "related_transaction_id",
                    "status",
                    "pending_balance",
                ],
            )
            writer.writeheader()
            for record in self.output_records:
                writer.writerow(
                    {
                        "transaction_id": record.get("transaction_id"),
                        "card_id": record.get("card_id"),
                        "merchant_name": record.get("merchant_name"),
                        "timestamp": record.get("timestamp"),
                        "amount": record.get("amount"),
                        "location": record.get("location"),
                        "transaction_type": record.get("transaction_type"),
                        "related_transaction_id": record.get("related_transaction_id"),
                        "status": record.get("validated_status"),
                        "pending_balance": record.get("pending_balance"),
                    }
                )

        print("Saved all processed transactions to results/stream_transactions.csv")


if __name__ == "__main__":
    consumer = KafkaTransactionConsumerSimpleReal()
    consumer.start()
