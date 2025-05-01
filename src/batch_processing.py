import csv
import os
from collections import defaultdict
from helper import calculate_credit_score_adjustment, calculate_new_credit_limit


class BatchProcessor:
    def __init__(self):
        self.transactions = []
        self.cards = {}
        self.customers = {}

    def load_csv(self, path):
        with open(path, newline="") as f:
            return list(csv.DictReader(f))

    def load_data(self):
        self.transactions = self.load_csv("results/stream_transactions.csv")
        for row in self.load_csv("data/cards.csv"):
            row["card_id"] = int(float(row["card_id"]))
            row["customer_id"] = int(float(row["customer_id"]))
            row["current_balance"] = float(row["current_balance"])
            row["credit_limit"] = int(float(row["credit_limit"]))
            self.cards[row["card_id"]] = row

        for row in self.load_csv("data/customers.csv"):
            row["customer_id"] = int(float(row["customer_id"]))
            row["credit_score"] = int(float(row["credit_score"]))
            row["annual_income"] = int(float(row["annual_income"]))
            self.customers[row["customer_id"]] = row

    def approve_pending_transactions(self):
        self.approved = []
        for tx in self.transactions:
            if tx["status"] == "pending":
                tx_copy = tx.copy()
                tx_copy["status"] = "approved"  # update status
                self.approved.append(tx_copy)

        with open("results/batch_transactions.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.approved[0].keys())
            writer.writeheader()
            writer.writerows(self.approved)

    def update_card_balances(self):
        for tx in self.approved:
            card_id = int(tx["card_id"])
            amount = float(tx["amount"])
            self.cards[card_id]["current_balance"] += amount

    def update_customers_and_limits(self):
        card_usage = defaultdict(lambda: {"balance": 0, "limit": 0})
        for card in self.cards.values():
            cid = card["customer_id"]
            card_usage[cid]["balance"] += card["current_balance"]
            card_usage[cid]["limit"] += card["credit_limit"]

        for cid, usage in card_usage.items():
            balance = usage["balance"]
            limit = usage["limit"]
            if limit == 0:
                usage_pct = 0
            else:
                usage_pct = (balance / limit) * 100

            score_change = calculate_credit_score_adjustment(usage_pct)
            old_score = self.customers[cid]["credit_score"]
            new_score = max(300, min(850, old_score + score_change))
            self.customers[cid]["credit_score"] = new_score

            # Reduce card limits if score dropped
            if score_change < 0:
                for card in self.cards.values():
                    if card["customer_id"] == cid:
                        old_limit = card["credit_limit"]
                        card["credit_limit"] = calculate_new_credit_limit(
                            old_limit, score_change
                        )

    def save_updated_data(self):
        if not os.path.exists("results"):
            os.makedirs("results")

        with open("results/cards_updated.csv", "w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=self.cards[next(iter(self.cards))].keys()
            )
            writer.writeheader()
            rounded_cards = []
            for card in self.cards.values():
                card_copy = card.copy()
                card_copy["current_balance"] = round(card_copy["current_balance"], 2)
                card_copy["credit_limit"] = float(card_copy["credit_limit"])
                rounded_cards.append(card_copy)

            writer.writerows(rounded_cards)

        with open("results/customers_updated.csv", "w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=self.customers[next(iter(self.customers))].keys()
            )
            writer.writeheader()
            formatted_customers = []
            for customer in self.customers.values():
                customer_copy = customer.copy()
                customer_copy["annual_income"] = float(customer_copy["annual_income"])
                formatted_customers.append(customer_copy)
            writer.writerows(formatted_customers)

    def run(self):
        print("Running batch layer...")
        self.load_data()
        self.approve_pending_transactions()
        self.update_card_balances()
        self.update_customers_and_limits()
        self.save_updated_data()
        print("Batch processing complete. Results saved to /results/")


if __name__ == "__main__":
    processor = BatchProcessor()
    processor.run()
