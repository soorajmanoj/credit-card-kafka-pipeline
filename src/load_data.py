import csv
import datetime
from utils import DataProcessor


class SimpleDataLoader:
    def __init__(self):
        self.dp = DataProcessor()
        self.conn = self.dp.get_mysql_connection()
        self.cursor = self.conn.cursor()

    def parse_date_mm_yy(self, mm_yy):
        try:
            return datetime.datetime.strptime(mm_yy.strip(), "%m/%y").strftime(
                "%Y-%m-01"
            )
        except:
            return None

    def load_customers(self, path):
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.cursor.execute(
                    """
                    INSERT INTO customers (
                        customer_id, name, phone_number, address,
                        email, credit_score, annual_income
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        int(row["customer_id"]),
                        row["name"],
                        row["phone_number"],
                        row["address"],
                        row["email"],
                        float(row["credit_score"]),
                        float(row["annual_income"]),
                    ),
                )
        self.conn.commit()
        print("✅ customers loaded.")

    def load_cards(self, path):
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                exp_date = self.parse_date_mm_yy(row["expiration_date"])
                self.cursor.execute(
                    """
                    INSERT INTO cards (
                        card_id, customer_id, card_type_id, card_number,
                        expiration_date, credit_limit, current_balance, issue_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        int(row["card_id"]),
                        int(row["customer_id"]),
                        int(row["card_type_id"]),
                        row["card_number"],
                        exp_date,
                        float(row["credit_limit"]),
                        float(row["current_balance"]),
                        row["issue_date"],
                    ),
                )
        self.conn.commit()
        print("✅ cards loaded.")

    def load_credit_card_types(self, path):
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.cursor.execute(
                    """
                    INSERT INTO credit_card_types (
                        card_type_id, name, credit_score_min, credit_score_max,
                        credit_limit_min, credit_limit_max, annual_fee, rewards_rate
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        int(row["card_type_id"]),
                        row["name"],
                        int(row["credit_score_min"]),
                        int(row["credit_score_max"]),
                        int(row["credit_limit_min"]),
                        int(row["credit_limit_max"]),
                        int(row["annual_fee"]),
                        float(row["rewards_rate"]),
                    ),
                )
        self.conn.commit()
        print("✅ credit_card_types loaded.")

    def load_transactions(self, path):
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.cursor.execute(
                    """
                    INSERT INTO transactions (
                        transaction_id, card_id, merchant_name, timestamp,
                        amount, location, transaction_type, related_transaction_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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
                    ),
                )
        self.conn.commit()
        print("✅ transactions loaded.")

    def run_all_loads(self):
        self.load_customers("data/customers.csv")
        self.load_cards("data/cards.csv")
        self.load_credit_card_types("data/credit_card_types.csv")
        self.load_transactions("data/transactions.csv")
        self.cursor.close()
        self.conn.close()


if __name__ == "__main__":
    loader = SimpleDataLoader()
    loader.run_all_loads()
