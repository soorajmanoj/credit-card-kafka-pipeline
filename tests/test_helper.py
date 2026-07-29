"""
Tests for the pure business-logic functions in src/helper.py:
location proximity check, credit score adjustment, and credit limit
recalculation. These are the functions the batch and stream layers
both depend on for approve/decline and reconciliation decisions.
"""

from helper import (
    is_location_close_enough,
    calculate_credit_score_adjustment,
    calculate_new_credit_limit,
)


# -- is_location_close_enough ------------------------------------------------

def test_location_same_zip_prefix_is_close():
    assert is_location_close_enough("12345", "12399") is True


def test_location_same_first_digit_only_is_still_close():
    # Current logic treats same-first-digit as close enough too.
    assert is_location_close_enough("12345", "19999") is True


def test_location_different_first_digit_is_far():
    assert is_location_close_enough("12345", "99999") is False


def test_location_missing_zip_is_rejected():
    assert is_location_close_enough(None, "12345") is False
    assert is_location_close_enough("12345", None) is False


def test_location_short_zip_is_rejected():
    assert is_location_close_enough("123", "12345") is False


# -- calculate_credit_score_adjustment ---------------------------------------

def test_score_adjustment_excellent_utilization():
    assert calculate_credit_score_adjustment(5) == 15


def test_score_adjustment_very_good_utilization():
    assert calculate_credit_score_adjustment(15) == 10


def test_score_adjustment_good_utilization():
    assert calculate_credit_score_adjustment(30) == 5


def test_score_adjustment_fair_utilization():
    assert calculate_credit_score_adjustment(40) == -5


def test_score_adjustment_high_utilization():
    assert calculate_credit_score_adjustment(60) == -15


def test_score_adjustment_very_high_utilization():
    assert calculate_credit_score_adjustment(90) == -25


def test_score_adjustment_boundary_at_thresholds():
    # Boundaries are inclusive on the lower (better) side of each bracket.
    assert calculate_credit_score_adjustment(10) == 15
    assert calculate_credit_score_adjustment(20) == 10
    assert calculate_credit_score_adjustment(50) == -5
    assert calculate_credit_score_adjustment(70) == -15


# -- calculate_new_credit_limit ----------------------------------------------

def test_limit_unchanged_when_score_improves():
    assert calculate_new_credit_limit(1000, 10) == 1000


def test_limit_unchanged_when_score_flat():
    assert calculate_new_credit_limit(1000, 0) == 1000


def test_limit_small_drop_reduces_five_percent():
    # 1200 * 0.95 = 1140 -> rounds to nearest hundred -> 1100.
    # (Using 1000 here would hit a round-half-to-even tie at 950; 1200
    # avoids that so the 5% reduction is unambiguous.)
    assert calculate_new_credit_limit(1200, -5) == 1100


def test_limit_moderate_drop_reduces_ten_percent():
    assert calculate_new_credit_limit(1000, -10) == 900


def test_limit_significant_drop_reduces_fifteen_percent():
    # 1200 * 0.85 = 1020 -> rounds to nearest hundred -> 1000.
    assert calculate_new_credit_limit(1200, -20) == 1000


def test_limit_rounds_to_nearest_hundred():
    # 837.5 -> 15% off 985 -> round(-100) should land on a clean hundred.
    result = calculate_new_credit_limit(985, -20)
    assert result % 100 == 0
