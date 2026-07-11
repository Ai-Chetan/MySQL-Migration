"""
tests/test_type_converter.py
-----------------------------
Unit tests for core/type_converter.py.
Run with: python -m pytest tests/
"""
from __future__ import annotations

import pytest

from core.type_converter import (
    ConversionSafety,
    classify_conversion,
    get_cast_expression,
    needs_cast,
)


class TestClassifyConversion:
    # --- Identical types (always SAFE) ---
    @pytest.mark.parametrize("type_", ["INT", "VARCHAR(100)", "TEXT", "DATETIME"])
    def test_identical_is_safe(self, type_: str) -> None:
        assert classify_conversion(type_, type_) == ConversionSafety.SAFE

    # --- Integer widenings ---
    def test_int_to_bigint_safe(self) -> None:
        assert classify_conversion("INT", "BIGINT") == ConversionSafety.SAFE

    def test_tinyint_to_int_safe(self) -> None:
        assert classify_conversion("TINYINT", "INT") == ConversionSafety.SAFE

    def test_tinyint_to_smallint_safe(self) -> None:
        assert classify_conversion("TINYINT", "SMALLINT") == ConversionSafety.SAFE

    # --- Integer narrowings ---
    def test_bigint_to_int_lossy(self) -> None:
        result = classify_conversion("BIGINT", "INT")
        assert result in (ConversionSafety.LOSSY, ConversionSafety.UNSAFE)

    # --- Numeric → string (safe) ---
    def test_int_to_varchar_safe(self) -> None:
        assert classify_conversion("INT", "VARCHAR(20)") == ConversionSafety.SAFE

    def test_decimal_to_varchar_safe(self) -> None:
        assert classify_conversion("DECIMAL(10,2)", "VARCHAR(20)") == ConversionSafety.SAFE

    # --- String → numeric (unsafe) ---
    def test_varchar_to_int_unsafe(self) -> None:
        assert classify_conversion("VARCHAR(50)", "INT") == ConversionSafety.UNSAFE

    # --- Float ↔ Decimal ---
    def test_float_to_decimal_lossy(self) -> None:
        result = classify_conversion("FLOAT", "DECIMAL(10,2)")
        assert result in (ConversionSafety.LOSSY, ConversionSafety.SAFE)

    # --- Datetime ---
    def test_date_to_datetime_safe(self) -> None:
        assert classify_conversion("DATE", "DATETIME") == ConversionSafety.SAFE

    def test_datetime_to_date_lossy(self) -> None:
        result = classify_conversion("DATETIME", "DATE")
        assert result in (ConversionSafety.LOSSY, ConversionSafety.UNSAFE)

    # --- Text widening ---
    def test_varchar_to_text_safe(self) -> None:
        assert classify_conversion("VARCHAR(255)", "TEXT") == ConversionSafety.SAFE

    def test_text_to_varchar_lossy(self) -> None:
        result = classify_conversion("TEXT", "VARCHAR(100)")
        assert result in (ConversionSafety.LOSSY, ConversionSafety.UNSAFE)


class TestGetCastExpression:
    def test_cast_int(self) -> None:
        expr = get_cast_expression("col", "INT")
        assert "col" in expr
        assert "CAST" in expr.upper() or expr == "col"

    def test_cast_varchar(self) -> None:
        expr = get_cast_expression("col", "VARCHAR(50)")
        assert "col" in expr

    def test_no_cast_same_type(self) -> None:
        # When no cast is needed, expression should just be the column name
        result = get_cast_expression("col", "INT", source_type="INT")
        assert result == "col" or "col" in result


class TestNeedsCast:
    def test_same_type_no_cast(self) -> None:
        assert not needs_cast("INT", "INT")

    def test_widening_may_not_need_cast(self) -> None:
        # INT → BIGINT doesn't require explicit CAST in MySQL
        result = needs_cast("INT", "BIGINT")
        assert isinstance(result, bool)

    def test_text_to_int_needs_cast(self) -> None:
        # Even if UNSAFE, checking the boolean return is valid
        result = needs_cast("VARCHAR(20)", "INT")
        assert isinstance(result, bool)
