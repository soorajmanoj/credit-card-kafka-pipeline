import csv
import os
from collections import defaultdict


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

    def calculate_credit_score_adjustment(self, usage_percentage):
        """
        Calculate credit score adjustment based on credit usage percentage.

        Args:
            usage_percentage: Credit usage as a percentage of total available credit (0-100)

        Returns:
            int: Credit score adjustment (positive or negative)
        """
        # Credit utilization best practices suggest keeping usage below 30%
        if usage_percentage <= 10:
            # Excellent utilization: significant score improvement
            return 15
        elif usage_percentage <= 20:
            # Very good utilization
            return 10
        elif usage_percentage <= 30:
            # Good utilization
            return 5
        elif usage_percentage <= 50:
            # Fair utilization: small penalty
            return -5
        elif usage_percentage <= 70:
            # High utilization: moderate penalty
            return -15
        else:
            # Very high utilization: significant penalty
            return -25

    def calculate_new_credit_limit(self, old_limit, credit_score_change):
        """
        Calculate new credit limit based on credit score changes.

        Args:
            old_limit: Current credit limit
            credit_score_change: Amount the credit score changed

        Returns:
            float: New credit limit
        """
        # Only reduce limits when scores drop
        if credit_score_change >= 0:
            return old_limit

        # Calculate percentage reduction based on score drop
        if credit_score_change <= -20:
            # Significant drop: reduce by 15%
            reduction_factor = 0.85
        elif credit_score_change <= -10:
            # Moderate drop: reduce by 10%
            reduction_factor = 0.90
        else:
            # Small drop: reduce by 5%
            reduction_factor = 0.95

        return round(old_limit * reduction_factor, -2)  # Round to nearest 100

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

            score_change = self.calculate_credit_score_adjustment(usage_pct)
            old_score = self.customers[cid]["credit_score"]
            new_score = max(300, min(850, old_score + score_change))
            self.customers[cid]["credit_score"] = new_score

            # Reduce card limits if score dropped
            if score_change < 0:
                for card in self.cards.values():
                    if card["customer_id"] == cid:
                        old_limit = card["credit_limit"]
                        card["credit_limit"] = self.calculate_new_credit_limit(
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
