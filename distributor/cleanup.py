"""
Cleanup utilities for the distributor.
"""

import aiosqlite
from loguru import logger
from typing import Optional


async def merge_pending_transfers(db_path: str = "database.db") -> None:
    """
    Merge all pending transfers in the database based on their origin and destination coldkeys.
    After merging, set the batch ID to the latest completed move transaction batch ID.
    If no completed moves exist, use the default batch ID: '12703824-6cb0-4ddd-9a33-bdebeca60f01'
    """
    try:
        logger.info("Starting merge of pending transfers...")

        async with aiosqlite.connect(db_path) as db:
            # First, get the latest completed move transaction batch ID
            latest_batch_cursor = await db.execute("""
                SELECT batch_id FROM moves 
                WHERE status = 'completed' 
                ORDER BY updated_at DESC 
                LIMIT 1
            """)
            latest_batch_row = await latest_batch_cursor.fetchone()

            if latest_batch_row:
                target_batch_id = latest_batch_row[0]
                logger.info(f"Found latest completed move batch ID: {target_batch_id}")
            else:
                target_batch_id = "12703824-6cb0-4ddd-9a33-bdebeca60f01"
                logger.info(
                    f"No completed moves found, using default batch ID: {target_batch_id}"
                )

            # Get all pending transfers grouped by origin_coldkey and destination_coldkey
            pending_cursor = await db.execute("""
                SELECT origin_coldkey, destination_coldkey, destination_h160, origin_hotkey,
                       SUM(amount) as total_amount, COUNT(*) as transfer_count,
                       MIN(created_at) as earliest_created, MAX(updated_at) as latest_updated
                FROM transfers 
                WHERE status = 'pending'
                GROUP BY origin_coldkey, destination_coldkey, destination_h160, origin_hotkey
                HAVING COUNT(*) > 0
            """)

            pending_groups = await pending_cursor.fetchall()

            if not pending_groups:
                logger.info("No pending transfers found to merge")
                return

            logger.info(f"Found {len(pending_groups)} groups of transfers to merge")

            merged_count = 0
            total_transfers_processed = 0

            for group in pending_groups:
                (
                    origin_coldkey,
                    destination_coldkey,
                    destination_h160,
                    origin_hotkey,
                    total_amount,
                    transfer_count,
                    earliest_created,
                    latest_updated,
                ) = group

                logger.debug(
                    f"Processing group: {origin_coldkey} -> {destination_coldkey}, "
                    f"total amount: {total_amount}, transfer count: {transfer_count}"
                )

                # Delete all existing pending transfers for this group
                delete_cursor = await db.execute(
                    """
                    DELETE FROM transfers 
                    WHERE status = 'pending' 
                    AND origin_coldkey = ? 
                    AND destination_coldkey = ?
                    AND destination_h160 = ?
                    AND origin_hotkey = ?
                """,
                    (
                        origin_coldkey,
                        destination_coldkey,
                        destination_h160,
                        origin_hotkey,
                    ),
                )

                # Insert the merged transfer with the target batch ID
                await db.execute(
                    """
                    INSERT INTO transfers (
                        batch_id, status, origin_coldkey, destination_coldkey, 
                        destination_h160, origin_hotkey, amount, created_at, updated_at
                    ) VALUES (?, 'pending', ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        target_batch_id,
                        origin_coldkey,
                        destination_coldkey,
                        destination_h160,
                        origin_hotkey,
                        total_amount,
                        earliest_created,
                        latest_updated,
                    ),
                )

                merged_count += 1
                total_transfers_processed += transfer_count

                logger.debug(
                    f"Merged {transfer_count} transfers into 1 for "
                    f"{origin_coldkey} -> {destination_coldkey} with amount {total_amount}"
                )

            # Commit the transaction
            await db.commit()

            logger.info(
                f"Successfully merged {total_transfers_processed} pending transfers into "
                f"{merged_count} consolidated transfers with batch ID: {target_batch_id}"
            )

    except Exception as e:
        logger.error(f"Error merging pending transfers: {e}")
        raise


async def get_pending_transfers_summary(db_path: str = "database.db") -> dict:
    """
    Get a summary of pending transfers for debugging/monitoring purposes.
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            # Get total pending transfers
            total_cursor = await db.execute("""
                SELECT COUNT(*), SUM(amount) FROM transfers WHERE status = 'pending'
            """)
            total_count, total_amount = await total_cursor.fetchone()

            # Get grouped summary
            groups_cursor = await db.execute("""
                SELECT origin_coldkey, destination_coldkey, COUNT(*), SUM(amount)
                FROM transfers 
                WHERE status = 'pending'
                GROUP BY origin_coldkey, destination_coldkey
            """)
            groups = await groups_cursor.fetchall()

            return {
                "total_pending_transfers": total_count or 0,
                "total_pending_amount": total_amount or 0.0,
                "unique_groups": len(groups),
                "groups": [
                    {
                        "origin_coldkey": group[0],
                        "destination_coldkey": group[1],
                        "transfer_count": group[2],
                        "total_amount": group[3],
                    }
                    for group in groups
                ],
            }
    except Exception as e:
        logger.error(f"Error getting pending transfers summary: {e}")
        return {}


if __name__ == "__main__":
    import asyncio

    async def main():
        # Get summary before merge
        logger.info("Getting summary before merge...")
        before_summary = await get_pending_transfers_summary()
        logger.info(f"Before merge: {before_summary}")

        # Perform merge
        await merge_pending_transfers()

        # Get summary after merge
        logger.info("Getting summary after merge...")
        after_summary = await get_pending_transfers_summary()
        logger.info(f"After merge: {after_summary}")

    asyncio.run(main())
