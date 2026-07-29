"""
Tests for the transaction validation logic in src/consumer.py.

KafkaTransactionConsumerSimpleReal connects to a real Kafka broker and MySQL
in __init__, so both are mocked here: KafkaConsumer is replaced with a dummy
that's never actually iterated, and DataProcessor.get_mysql_connection is
replaced with a MagicMock connection/cursor whose fetchone() results we
control per test. That isolates validate_transaction's actual decision
logic (decline rules, location check, pending-balance bookkeeping) from any
network dependency.
"""

from unittest.mock import MagicMock

import pytest

from utils import DataProcessor
import consumer as consumer_module
from consumer import KafkaTransactionConsumerSimpleReal


@pytest.fixture
def make_consumer(monkeypatch):
    """Returns a factory that builds a KafkaTransactionConsumerSimpleReal
    with Kafka/MySQL mocked, and lets each test control what the two
    cursor.fetchone() calls in validate_transaction (card lookup, then
    customer address lookup) return."""

    def _make(card_info, address_row=None):
        monkeypatch.setattr(consumer_module, "KafkaConsumer", lambda *a, **kw: MagicMock())

        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [card_info, address_row]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        monkeypatch.setattr(DataProcessor, "get_mysql_connection", lambda self: mock_conn)

        return KafkaTransactionConsumerSimpleReal()

    return _make


CLOSE_ADDRESS = {"address": "1 Main St, Springfield, IL 12345"}
FAR_ADDRESS = {"address": "1 Main St, Springfield, IL 99999"}


def test_refund_always_accepted_even_over_limit(make_consumer):
    card_info = {"customer_id": 1, "credit_limit": 100.0, "current_balance": 0.0}
    c = make_consumer(card_info, CLOSE_ADDRESS)

    record = {
        "transaction_id": "1", "card_id": "1", "amount": 500.0,
        "location": "1 Main St, Springfield, IL 12345", "transaction_type": "refund",
    }
    c.validate_transaction(record)

    assert record["validated_status"] == "pending"
    assert c.output_records[-1]["transaction_id"] == "1"


def test_amount_over_half_limit_is_declined(make_consumer):
    card_info = {"customer_id": 1, "credit_limit": 100.0, "current_balance": 0.0}
    c = make_consumer(card_info, CLOSE_ADDRESS)

    record = {
        "transaction_id": "2", "card_id": "1", "amount": 60.0,
        "location": "1 Main St, Springfield, IL 12345", "transaction_type": "purchase",
    }
    c.validate_transaction(record)

    assert record["validated_status"] == "declined"


def test_amount_plus_pending_over_limit_is_declined(make_consumer):
    card_info = {"customer_id": 1, "credit_limit": 100.0, "current_balance": 90.0}
    c = make_consumer(card_info, CLOSE_ADDRESS)

    record = {
        "transaction_id": "3", "card_id": "1", "amount": 20.0,
        "location": "1 Main St, Springfield, IL 12345", "transaction_type": "bill_payment",
    }
    c.validate_transaction(record)

    assert record["validated_status"] == "declined"


def test_purchase_far_from_customer_is_declined(make_consumer):
    card_info = {"customer_id": 1, "credit_limit": 1000.0, "current_balance": 0.0}
    c = make_consumer(card_info, FAR_ADDRESS)

    record = {
        "transaction_id": "4", "card_id": "1", "amount": 10.0,
        "location": "1 Main St, Somewhere, TX 88888", "transaction_type": "purchase",
    }
    c.validate_transaction(record)

    assert record["validated_status"] == "declined"


def test_purchase_close_to_customer_is_accepted(make_consumer):
    card_info = {"customer_id": 1, "credit_limit": 1000.0, "current_balance": 0.0}
    c = make_consumer(card_info, CLOSE_ADDRESS)

    record = {
        "transaction_id": "5", "card_id": "1", "amount": 10.0,
        "location": "1 Main St, Springfield, IL 12345", "transaction_type": "purchase",
    }
    c.validate_transaction(record)

    assert record["validated_status"] == "pending"


def test_pending_balance_accumulates_across_transactions(make_consumer):
    card_info = {"customer_id": 1, "credit_limit": 1000.0, "current_balance": 100.0}
    c = make_consumer(card_info, CLOSE_ADDRESS)
    # Second call needs its own fetchone results too.
    c.cursor.fetchone.side_effect = [card_info, CLOSE_ADDRESS, card_info, CLOSE_ADDRESS]

    record1 = {
        "transaction_id": "6", "card_id": "1", "amount": 10.0,
        "location": "1 Main St, Springfield, IL 12345", "transaction_type": "purchase",
    }
    record2 = {
        "transaction_id": "7", "card_id": "1", "amount": 15.0,
        "location": "1 Main St, Springfield, IL 12345", "transaction_type": "purchase",
    }
    c.validate_transaction(record1)
    c.validate_transaction(record2)

    # 100 (starting balance) + 10 + 15 = 125
    assert record2["pending_balance"] == pytest.approx(125.0)


def test_unknown_card_is_skipped(make_consumer):
    c = make_consumer(card_info=None, address_row=None)

    record = {
        "transaction_id": "8", "card_id": "999", "amount": 10.0,
        "location": "1 Main St, Springfield, IL 12345", "transaction_type": "purchase",
    }
    c.validate_transaction(record)

    assert c.output_records == []
