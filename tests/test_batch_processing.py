"""
Tests for BatchProcessor in src/batch_processing.py.

BatchProcessor doesn't touch Kafka or MySQL directly — it only reads/writes
CSVs under results/ and data/ — so it's tested here by constructing it and
setting .transactions/.cards/.customers directly (the same shape load_data()
would produce), skipping the CSV-loading step. Each test chdir's into a
pytest tmp_path so the results/*.csv files it writes don't touch the repo.
"""

import csv
import os

import pytest

from batch_processing import BatchProcessor


def make_processor():
    bp = BatchProcessor()
    bp.cards = {
        1: {"card_id": 1, "customer_id": 100, "current_balance": 500.0, "credit_limit": 1000},
        2: {"card_id": 2, "customer_id": 100, "current_balance": 200.0, "credit_limit": 1000},
        3: {"card_id": 3, "customer_id": 200, "current_balance": 50.0, "credit_limit": 1000},
    }
    bp.customers = {
        100: {"customer_id": 100, "credit_score": 700, "annual_income": 80000},
        200: {"customer_id": 200, "credit_score": 700, "annual_income": 60000},
    }
    bp.transactions = [
        {"transaction_id": "1", "card_id": "1", "amount": "100.0", "status": "pending"},
        {"transaction_id": "2", "card_id": "3", "amount": "50.0", "status": "declined"},
        {"transaction_id": "3", "card_id": "2", "amount": "25.0", "status": "pending"},
    ]
    return bp


def test_approve_pending_transactions_filters_and_writes(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    os.makedirs("results")
    bp = make_processor()

    bp.approve_pending_transactions()

    # Only the two "pending" rows should be approved — declined stays out.
    assert len(bp.approved) == 2
    assert all(tx["status"] == "approved" for tx in bp.approved)
    assert {tx["transaction_id"] for tx in bp.approved} == {"1", "3"}

    with open("results/batch_transactions.csv", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 2
    assert all(r["status"] == "approved" for r in rows)


def test_update_card_balances_applies_approved_amounts(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    os.makedirs("results")
    bp = make_processor()
    bp.approve_pending_transactions()

    bp.update_card_balances()

    # Card 1 got +100 (500 -> 600), card 2 got +25 (200 -> 225).
    assert bp.cards[1]["current_balance"] == pytest.approx(600.0)
    assert bp.cards[2]["current_balance"] == pytest.approx(225.0)
    # Card 3's transaction was declined, so its balance is untouched.
    assert bp.cards[3]["current_balance"] == pytest.approx(50.0)


def test_low_usage_customer_score_improves_and_limit_untouched():
    """Usage well under 10% -> best score bracket -> limit is only ever
    reduced on a *drop*, so an improving score leaves the limit alone."""
    bp = BatchProcessor()
    bp.cards = {
        3: {"card_id": 3, "customer_id": 200, "current_balance": 50.0, "credit_limit": 1000},
    }
    bp.customers = {
        200: {"customer_id": 200, "credit_score": 700, "annual_income": 60000},
    }
    bp.approved = []  # not exercised by this method directly

    bp.update_customers_and_limits()

    assert bp.customers[200]["credit_score"] == 715  # 700 + 15
    assert bp.cards[3]["credit_limit"] == 1000


def test_moderate_usage_customer_score_drops_and_limit_shrinks():
    """37.5% combined usage across two cards lands in the 'fair' bracket
    (-5), which triggers a 5% limit reduction on every card of that
    customer. Limits chosen to land cleanly away from a rounding tie."""
    bp = BatchProcessor()
    bp.cards = {
        1: {"card_id": 1, "customer_id": 100, "current_balance": 500.0, "credit_limit": 1200},
        2: {"card_id": 2, "customer_id": 100, "current_balance": 400.0, "credit_limit": 1200},
    }
    bp.customers = {
        100: {"customer_id": 100, "credit_score": 700, "annual_income": 80000},
    }
    bp.approved = []

    bp.update_customers_and_limits()

    # combined balance 900 / combined limit 2400 = 37.5% -> "fair" -> -5
    assert bp.customers[100]["credit_score"] == 695
    # 1200 * 0.95 = 1140 -> rounds to nearest hundred -> 1100
    assert bp.cards[1]["credit_limit"] == 1100
    assert bp.cards[2]["credit_limit"] == 1100


def test_credit_score_is_clamped_to_valid_range():
    bp = BatchProcessor()
    bp.cards = {
        1: {"card_id": 1, "customer_id": 100, "current_balance": 10.0, "credit_limit": 1000},
    }
    bp.customers = {
        100: {"customer_id": 100, "credit_score": 845, "annual_income": 80000},
    }
    bp.approved = []

    bp.update_customers_and_limits()

    assert bp.customers[100]["credit_score"] <= 850


def test_save_updated_data_writes_both_csvs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    os.makedirs("results")
    bp = make_processor()
    bp.approve_pending_transactions()
    bp.update_card_balances()
    bp.update_customers_and_limits()

    bp.save_updated_data()

    with open("results/cards_updated.csv", newline="") as f:
        card_rows = list(csv.DictReader(f))
    with open("results/customers_updated.csv", newline="") as f:
        customer_rows = list(csv.DictReader(f))

    assert len(card_rows) == 3
    assert len(customer_rows) == 2
    assert {r["card_id"] for r in card_rows} == {"1", "2", "3"}
