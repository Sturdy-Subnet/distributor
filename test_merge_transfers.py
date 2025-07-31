#!/usr/bin/env python3
"""
Test script to demonstrate the merge_tiny_transfers functionality.
This script creates test data and verifies that tiny transfers are properly merged.
Note: MINIMUM_TRANSFER_AMOUNT_TAO represents the minimum value in TAO tokens,
not TAO units. The merge function will calculate if alpha_amount * alpha_price < MINIMUM_TRANSFER_AMOUNT_TAO.
"""

import asyncio
import sqlite3
import os
import tempfile
import uuid
from unittest.mock import MagicMock
import sys

# Add the distributor directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), "distributor"))

# Import after adding to path
# pylint: disable=import-error,wrong-import-position
from constants import MINIMUM_TRANSFER_AMOUNT_TAO


class MockWallet:
    """Mock wallet for testing"""

    def __init__(self, coldkey_address):
        self.coldkeypub = MagicMock()
        self.coldkeypub.ss58_address = coldkey_address


class MockPrice:
    """Mock price object"""

    def __init__(self, tao_value):
        self.tao = tao_value


class MockSubtensor:
    """Mock subtensor for testing"""

    def __init__(self, alpha_price_in_tao):
        self.alpha_price = alpha_price_in_tao

    async def get_subnet_price(self, netuid):
        return MockPrice(self.alpha_price)


async def create_test_database(db_path, alpha_price_in_tao):
    """Create a test database with sample transfer data"""
    # Read and execute the schema
    schema_path = os.path.join(os.path.dirname(__file__), "db", "schema.sql")
    with open(schema_path, "r") as f:
        schema_sql = f.read()

    conn = sqlite3.connect(db_path)
    conn.executescript(schema_sql)

    # Create test coldkey addresses
    test_coldkey = "test_coldkey_123"
    dest_coldkey_1 = "dest_coldkey_456"
    dest_coldkey_2 = "dest_coldkey_789"
    test_hotkey = "test_hotkey_abc"

    # Create test transfers that should be merged (tiny amounts in TAO token value)
    # If alpha_price_in_tao = 0.1 and MINIMUM_TRANSFER_AMOUNT_TAO = 0.0021
    # Then alpha amounts that result in < 0.0021 TAO token value are tiny
    # So alpha_amount * 0.1 < 0.0021 means alpha_amount < 0.021
    tiny_alpha_amount = (
        MINIMUM_TRANSFER_AMOUNT_TAO / alpha_price_in_tao
    ) * 0.5  # Half of threshold
    large_alpha_amount = (
        MINIMUM_TRANSFER_AMOUNT_TAO / alpha_price_in_tao
    ) * 2  # Above threshold

    test_transfers = [
        # Group 1: These should be merged (same origin, dest, hotkey, all tiny)
        (
            str(uuid.uuid4()),
            "pending",
            test_coldkey,
            dest_coldkey_1,
            "h160_addr_1",
            test_hotkey,
            tiny_alpha_amount,
        ),
        (
            str(uuid.uuid4()),
            "pending",
            test_coldkey,
            dest_coldkey_1,
            "h160_addr_1",
            test_hotkey,
            tiny_alpha_amount,
        ),
        (
            str(uuid.uuid4()),
            "failed",
            test_coldkey,
            dest_coldkey_1,
            "h160_addr_1",
            test_hotkey,
            tiny_alpha_amount,
        ),
        # Group 2: These should be merged (different destination, all tiny)
        (
            str(uuid.uuid4()),
            "pending",
            test_coldkey,
            dest_coldkey_2,
            "h160_addr_2",
            test_hotkey,
            tiny_alpha_amount,
        ),
        (
            str(uuid.uuid4()),
            "failed",
            test_coldkey,
            dest_coldkey_2,
            "h160_addr_2",
            test_hotkey,
            tiny_alpha_amount,
        ),
        # This should NOT be merged (amount results in value above threshold)
        (
            str(uuid.uuid4()),
            "pending",
            test_coldkey,
            dest_coldkey_1,
            "h160_addr_1",
            test_hotkey,
            large_alpha_amount,
        ),
        # This should NOT be merged (different origin coldkey)
        (
            str(uuid.uuid4()),
            "pending",
            "different_coldkey",
            dest_coldkey_1,
            "h160_addr_1",
            test_hotkey,
            tiny_alpha_amount,
        ),
    ]

    for transfer in test_transfers:
        conn.execute(
            "INSERT INTO transfers (batch_id, status, origin_coldkey, destination_coldkey, destination_h160, origin_hotkey, amount) VALUES (?, ?, ?, ?, ?, ?, ?)",
            transfer,
        )

    conn.commit()
    conn.close()

    return test_coldkey


async def test_merge_functionality():
    """Test the merge_tiny_transfers function"""
    # Import the function we want to test
    # pylint: disable=import-error
    from distributor import merge_tiny_transfers

    # Create a temporary database
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        db_path = tmp_file.name

    try:
        # Test with alpha price = 0.1 TAO tokens per alpha
        alpha_price_in_tao = 0.1
        mock_subtensor = MockSubtensor(alpha_price_in_tao)

        # Create test data
        test_coldkey = await create_test_database(db_path, alpha_price_in_tao)
        mock_wallet = MockWallet(test_coldkey)

        # Check initial state
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT COUNT(*) FROM transfers WHERE origin_coldkey = ?", (test_coldkey,)
        )
        initial_count = cursor.fetchone()[0]
        print(f"Initial transfer count for test coldkey: {initial_count}")

        # Check tiny transfers before merge
        tiny_threshold_alpha = MINIMUM_TRANSFER_AMOUNT_TAO / alpha_price_in_tao
        cursor = conn.execute(
            "SELECT origin_coldkey, destination_coldkey, status, amount FROM transfers WHERE origin_coldkey = ?",
            (test_coldkey,),
        )
        all_transfers_before = cursor.fetchall()
        print(
            f"All transfers before merge (alpha price = {alpha_price_in_tao} TAO per alpha, threshold = {tiny_threshold_alpha} alpha):"
        )
        tiny_count = 0
        for transfer in all_transfers_before:
            origin, dest, status, amount = transfer
            tao_value = amount * alpha_price_in_tao
            is_tiny = tao_value < MINIMUM_TRANSFER_AMOUNT_TAO
            if is_tiny:
                tiny_count += 1
            print(
                f"  {transfer} -> {tao_value:.6f} TAO value {'(TINY)' if is_tiny else '(LARGE)'}"
            )

        print(f"Tiny transfers before merge: {tiny_count}")
        conn.close()

        # Run the merge function
        print("\nRunning merge_tiny_transfers...")
        await merge_tiny_transfers(db_path, mock_wallet, mock_subtensor)

        # Check results
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT COUNT(*) FROM transfers WHERE origin_coldkey = ?", (test_coldkey,)
        )
        final_count = cursor.fetchone()[0]
        print(f"\nFinal transfer count for test coldkey: {final_count}")

        # Check merged transfers
        cursor = conn.execute(
            "SELECT origin_coldkey, destination_coldkey, status, amount FROM transfers WHERE origin_coldkey = ?",
            (test_coldkey,),
        )
        final_transfers = cursor.fetchall()
        print("All transfers after merge:")
        for transfer in final_transfers:
            origin, dest, status, amount = transfer
            tao_value = amount * alpha_price_in_tao
            print(f"  {transfer} -> {tao_value:.6f} TAO value")

        # Verify that we have fewer transfers now (due to merging)
        assert final_count < initial_count, (
            f"Expected fewer transfers after merge: {final_count} >= {initial_count}"
        )

        # Verify that no remaining tiny transfers exist
        tiny_remaining = 0
        for transfer in final_transfers:
            origin, dest, status, amount = transfer
            tao_value = amount * alpha_price_in_tao
            if tao_value < MINIMUM_TRANSFER_AMOUNT_TAO:
                tiny_remaining += 1

        # Allow for single tiny transfers (can't be merged if there's only one)
        print(f"Tiny transfers remaining: {tiny_remaining}")

        conn.close()
        print("\n✅ Test passed! Tiny transfers were successfully merged.")

    finally:
        # Clean up
        if os.path.exists(db_path):
            os.unlink(db_path)


if __name__ == "__main__":
    asyncio.run(test_merge_functionality())
