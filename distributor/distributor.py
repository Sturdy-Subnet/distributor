import argparse
import asyncio
import dataclasses
from datetime import datetime, timedelta
import json
import bittensor as bt
from typing import Callable, Optional, List
from sturdy.utils.taofi_subgraph import get_fees_in_range
import aiosqlite
from zoneinfo import ZoneInfo
from loguru import logger
import sys
import os
import httpx
import dotenv
from web3 import AsyncWeb3
from args import add_args
from constants import (
    NETUID,
    SECONDS_PER_BT_BLOCK,
    MIN_REWARD_THRESHOLD,
    CHECK_INTERVAL_SECONDS,
    SCORE_CHECK_INTERVAL_SECONDS,
    PENDING_TRANSFER_WAIT_SECONDS,
    MIN_QUEUE_DISTRIBUTION_GAP,
    MIN_LOOKBACK_SECONDS,
    DEFAULT_LOOKBACK_SECONDS,
    WAIT_PERCENTAGE,
    FREQUENCY_PERCENTAGE_SCORE,
    FREQUENCY_PERCENTAGE_DISTRIBUTION,
    MIN_SCORE_RECORDS,
    TOKEN_IDS_FILE,
)
from addr import h160_to_ss58

# Load environment variables from .env file
dotenv.load_dotenv()


def time_matches(
    now: datetime,
    second: Optional[int] = None,
    minute: Optional[int] = None,
    hour: Optional[int] = None,
    days: Optional[List[int]] = None,
) -> bool:
    """Check if current time matches the schedule"""
    # If a component is None, it means "match any value"
    # If a component has a value, it must match exactly
    matches = True

    if second is not None:
        matches = matches and now.second == second
    if minute is not None:
        matches = matches and now.minute == minute
    if hour is not None:
        matches = matches and now.hour == hour
    if days is not None:
        matches = matches and now.weekday() in days

    return matches


def setup_logging(log_dir: str, log_level: str, log_rotation: str, log_retention: str):
    """Setup loguru logging with file output"""
    # Remove default handler
    logger.remove()

    # Add console handler with colored output
    logger.add(
        sys.stderr,
        level=log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss!UTC} UTC</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True,
    )

    # Create log directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)

    # Add file handler
    log_file_path = os.path.join(log_dir, "distributor_{time:YYYY-MM-DD}.log")
    logger.add(
        log_file_path,
        level=log_level,
        format="{time:YYYY-MM-DD HH:mm:ss!UTC} UTC | {level: <8} | {name}:{function}:{line} - {message}",
        rotation=log_rotation,
        retention=log_retention,
        compression="zip",
    )

    logger.info(f"Logging initialized - Level: {log_level}, Log dir: {log_dir}")


async def should_record_scores(
    db_path: str, current_block: int, frequency_secs: int
) -> tuple[bool, Optional[int]]:
    """
    Determine if we should record scores based on the last recorded block and frequency.

    Returns:
        tuple[bool, Optional[int]]: (should_record, next_record_block)
            - should_record: True if we should record scores now
            - next_record_block: The block number when we should next record scores (None if should_record is True)
    """
    try:
        frequency_blocks = frequency_secs // SECONDS_PER_BT_BLOCK

        async with aiosqlite.connect(db_path) as db:
            # Get the most recent score record
            async with db.execute(
                "SELECT MAX(block_end) FROM token_id_scores"
            ) as cursor:
                result = await cursor.fetchone()
                last_recorded_block = result[0] if result[0] is not None else 0

        # Calculate when we should next record scores
        next_record_block = last_recorded_block + frequency_blocks

        # If we haven't recorded anything yet, or enough blocks have passed
        if last_recorded_block == 0 or current_block >= next_record_block:
            logger.debug(
                f"Should record scores: current_block={current_block}, last_recorded={last_recorded_block}, frequency_blocks={frequency_blocks}"
            )
            return True, None
        else:
            blocks_remaining = next_record_block - current_block
            logger.debug(
                f"Skipping score recording: {blocks_remaining} blocks remaining until next window (next at block {next_record_block})"
            )
            return False, next_record_block

    except Exception as e:
        logger.error(f"Error checking if should record scores: {e}")
        # If we can't determine, default to recording
        return True, None


async def should_queue_distribution(
    db_path: str, current_time: datetime, distribution_schedule: dict, coldkey: str
) -> tuple[bool, Optional[datetime]]:
    """
    Determine if we should queue distribution based on the last distribution and schedule.

    Returns:
        tuple[bool, Optional[datetime]]: (should_queue, next_queue_time)
            - should_queue: True if we should queue distribution now
            - next_queue_time: The time when we should next queue distribution (None if should_queue is True)
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            # Get the most recent distribution queue time for this coldkey
            async with db.execute(
                "SELECT MAX(created_at) FROM transfers WHERE origin_coldkey = ?",
                (coldkey,),
            ) as cursor:
                result = await cursor.fetchone()
                last_queued_str = result[0] if result[0] is not None else None

        # If no previous distributions, we should queue
        if last_queued_str is None:
            logger.debug("No previous distributions found, should queue distribution")
            return True, None

        # Parse the last queued time and ensure timezone consistency
        if last_queued_str.endswith("Z"):
            # Handle UTC timestamps with Z suffix
            last_queued = datetime.fromisoformat(last_queued_str.replace("Z", "+00:00"))
        elif "+" in last_queued_str or last_queued_str.count("-") > 2:
            # Already has timezone info
            last_queued = datetime.fromisoformat(last_queued_str)
        else:
            # Assume UTC if no timezone info
            last_queued = datetime.fromisoformat(last_queued_str).replace(
                tzinfo=ZoneInfo("UTC")
            )

        # Ensure both datetimes have timezone info for comparison
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=ZoneInfo("UTC"))
        if last_queued.tzinfo is None:
            last_queued = last_queued.replace(tzinfo=ZoneInfo("UTC"))

        # Convert both to the same timezone for comparison
        target_tz = ZoneInfo(distribution_schedule.get("timezone", "UTC"))
        current_time = current_time.astimezone(target_tz)
        last_queued = last_queued.astimezone(target_tz)

        # Calculate next distribution time based on schedule
        if distribution_schedule.get("hour") is not None:
            # Time-based scheduling
            next_queue_time = calculate_next_scheduled_time(
                current_time, distribution_schedule
            )

            # Check if enough time has passed since last distribution
            if (
                current_time >= next_queue_time
                and (current_time - last_queued).total_seconds()
                >= MIN_QUEUE_DISTRIBUTION_GAP
            ):
                logger.debug(
                    f"Should queue distribution: current_time={current_time}, last_queued={last_queued}, next_queue_time={next_queue_time}"
                )
                return True, None
            else:
                logger.debug(
                    f"Skipping distribution queue: next scheduled at {next_queue_time}"
                )
                return False, next_queue_time
        else:
            # Frequency-based scheduling
            frequency_secs = distribution_schedule.get(
                "frequency_secs", DEFAULT_LOOKBACK_SECONDS
            )
            next_queue_time = last_queued + timedelta(seconds=frequency_secs)

            if current_time >= next_queue_time:
                logger.debug(
                    f"Should queue distribution: current_time={current_time}, last_queued={last_queued}, frequency={frequency_secs}s"
                )
                return True, None
            else:
                time_remaining = (next_queue_time - current_time).total_seconds()
                logger.debug(
                    f"Skipping distribution queue: {time_remaining:.0f}s remaining until next window"
                )
                return False, next_queue_time

    except Exception as e:
        logger.error(f"Error checking if should queue distribution: {e}")
        # If we can't determine, default to queuing
        return True, None


def calculate_next_scheduled_time(
    current_time: datetime, distribution_schedule: dict
) -> datetime:
    """Calculate the next scheduled distribution time based on the schedule."""
    try:
        timezone = ZoneInfo(distribution_schedule.get("timezone", "UTC"))
        hour = distribution_schedule.get("hour")
        minute = distribution_schedule.get("minute")
        second = distribution_schedule.get("second", 0)
        days = distribution_schedule.get("days")

        # Ensure current_time is in the target timezone
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=timezone)
        else:
            current_time = current_time.astimezone(timezone)

        # Start with the current time
        next_time = current_time.replace(microsecond=0)

        # Add one second to start from the next possible time
        next_time += timedelta(seconds=1)

        # Adjust components that are specified in the schedule
        if second is not None:
            if next_time.second > second:
                next_time += timedelta(minutes=1)
            next_time = next_time.replace(second=second)

        if minute is not None:
            if next_time.minute > minute:
                next_time += timedelta(hours=1)
            next_time = next_time.replace(minute=minute)

        if hour is not None:
            if next_time.hour > hour:
                next_time += timedelta(days=1)
            next_time = next_time.replace(hour=hour)

        # If specific days are configured, move to the next valid day
        if days is not None:
            while next_time.weekday() not in days:
                next_time += timedelta(days=1)
                # Reset time components when moving to a new day
                if hour is not None:
                    next_time = next_time.replace(hour=hour)
                if minute is not None:
                    next_time = next_time.replace(minute=minute)
                if second is not None:
                    next_time = next_time.replace(second=second)

        return next_time

    except Exception as e:
        logger.error(f"Error calculating next scheduled time: {e}")
        # Default to 1 minute from now
        return current_time + timedelta(minutes=1)


async def check_sufficient_scores_for_distribution(
    db_path: str, start_block: int, min_score_records: int = MIN_SCORE_RECORDS
) -> tuple[bool, int]:
    """
    Check if we have sufficient score records to perform a meaningful distribution.

    Returns:
        tuple[bool, int]: (sufficient_scores, record_count)
            - sufficient_scores: True if we have enough records
            - record_count: Number of score records found
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM token_id_scores WHERE block_start_stake >= ? AND used = 0",
                (start_block,),
            ) as cursor:
                result = await cursor.fetchone()
                record_count = result[0] if result[0] is not None else 0

        sufficient = record_count >= min_score_records
        logger.debug(
            f"Score records check: {record_count} records found since block {start_block}, sufficient: {sufficient}"
        )
        return sufficient, record_count

    except Exception as e:
        logger.error(f"Error checking score records: {e}")
        return False, 0


async def run_on_schedule(
    task: Callable,
    *,
    task_args: tuple = (),
    task_kwargs: dict = None,
    frequency_secs: Optional[int] = None,
    timezone: str = "UTC",
    hour: Optional[int] = None,
    minute: Optional[int] = None,
    second: Optional[int] = None,
    days: Optional[List[int]] = None,
):
    """
    Run the task either at the specified frequency in seconds or at specific scheduled times.
    Uses time-based scheduling if any time component (hour/minute/second/days) is specified.
    """
    if task_kwargs is None:
        task_kwargs = {}

    # Time-based scheduling if any time component is specified
    if any(x is not None for x in [hour, minute, second, days]):
        last_run_date = None
        # Fix: Handle None values in logging
        hour_str = "*" if hour is None else str(hour).zfill(2)
        minute_str = "*" if minute is None else str(minute).zfill(2)
        second_str = "*" if second is None else str(second).zfill(2)

        logger.info(
            f"Starting time-based scheduling for {task.__name__} at "
            f"hour={hour_str}, minute={minute_str}, second={second_str} {timezone}"
        )

        while True:
            now = datetime.now(ZoneInfo(timezone))

            if time_matches(now, second, minute, hour, days):
                if last_run_date is None or (now - last_run_date).total_seconds() >= 1:
                    logger.debug(f"Running scheduled task {task.__name__}")
                    await task(*task_args, **task_kwargs)
                    last_run_date = now

            # Calculate sleep time to next second
            sleep_time = 1 - (now.microsecond / 1_000_000)
            await asyncio.sleep(sleep_time)

    # Frequency-based scheduling
    else:
        if frequency_secs is None:
            raise ValueError("Must specify either time components or frequency_secs")

        logger.info(
            f"Starting frequency-based scheduling for {task.__name__} every {frequency_secs} seconds"
        )

        # Use intelligent scheduling for specific tasks
        if task.__name__ == "record_scores_for_distribution":
            await run_intelligent_score_recording(
                task, task_args, task_kwargs, frequency_secs
            )
        elif task.__name__ == "queue_distribution":
            await run_intelligent_distribution_queueing(
                task, task_args, task_kwargs, frequency_secs
            )
        else:
            while True:
                logger.debug(f"Running scheduled task {task.__name__}")
                await task(*task_args, **task_kwargs)
                await asyncio.sleep(frequency_secs)


async def run_intelligent_score_recording(
    task: Callable, task_args: tuple, task_kwargs: dict, frequency_secs: int
):
    """
    Intelligent scheduling for score recording that avoids duplicate records
    and optimizes timing based on blockchain state.
    """
    logger.info(
        f"Starting intelligent score recording with {frequency_secs}s frequency"
    )

    # Extract required parameters for should_record_scores
    db_path = task_kwargs.get("db_path")
    subtensor = task_kwargs.get("subtensor")

    if not db_path or not subtensor:
        logger.error("Missing required parameters for intelligent scheduling")
        return

    last_check_time = 0
    check_interval = SCORE_CHECK_INTERVAL_SECONDS

    while True:
        try:
            current_time = datetime.now().timestamp()

            # Only check blockchain state periodically to avoid excessive calls
            if current_time - last_check_time >= check_interval:
                current_block = await subtensor.get_current_block()
                should_record, next_record_block = await should_record_scores(
                    db_path, current_block, frequency_secs
                )

                if should_record:
                    logger.info(f"Recording scores at block {current_block}")
                    asyncio.create_task(task(*task_args, **task_kwargs))
                    # After recording, wait at least the minimum interval before checking again
                    last_check_time = current_time
                    await asyncio.sleep(
                        max(
                            SCORE_CHECK_INTERVAL_SECONDS,
                            frequency_secs * FREQUENCY_PERCENTAGE_SCORE,
                        )
                    )
                elif next_record_block:
                    # Calculate how long to wait based on blocks remaining
                    blocks_remaining = next_record_block - current_block
                    wait_seconds = max(
                        SCORE_CHECK_INTERVAL_SECONDS,
                        blocks_remaining * SECONDS_PER_BT_BLOCK * WAIT_PERCENTAGE,
                    )
                    logger.debug(
                        f"Next score recording in ~{blocks_remaining} blocks ({wait_seconds:.0f}s)"
                    )
                    last_check_time = current_time
                    await asyncio.sleep(min(wait_seconds, check_interval))
                else:
                    last_check_time = current_time
                    await asyncio.sleep(check_interval)
            else:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error in intelligent score recording: {e}")
            logger.exception(e)
            await asyncio.sleep(check_interval)


async def run_intelligent_distribution_queueing(
    task: Callable, task_args: tuple, task_kwargs: dict, frequency_secs: int
):
    """
    Intelligent scheduling for distribution queueing that avoids duplicate distributions
    and ensures sufficient data availability before queueing.
    """
    logger.info(
        f"Starting intelligent distribution queueing with {frequency_secs}s frequency"
    )

    # Extract required parameters
    db_path = task_kwargs.get("db_path")
    distribution_schedule = task_kwargs.get("distribution_schedule", {})
    pos_chain_subtensor = task_kwargs.get("pos_chain_subtensor")
    wallet = task_kwargs.get("wallet")

    if (
        not db_path
        or not distribution_schedule
        or not pos_chain_subtensor
        or not wallet
    ):
        logger.error(
            "Missing required parameters for intelligent distribution scheduling"
        )
        return

    coldkey = wallet.coldkeypub.ss58_address
    last_check_time = 0
    check_interval = CHECK_INTERVAL_SECONDS

    while True:
        try:
            current_time_ts = datetime.now().timestamp()
            current_time = datetime.now(
                ZoneInfo(distribution_schedule.get("timezone", "UTC"))
            )

            # Only check periodically to avoid excessive calls
            if current_time_ts - last_check_time >= check_interval:
                should_queue, next_queue_time = await should_queue_distribution(
                    db_path, current_time, distribution_schedule, coldkey
                )

                if should_queue:
                    # Additional check: ensure we have sufficient score data
                    current_block = await pos_chain_subtensor.get_current_block()

                    # Calculate how far back to look based on distribution schedule
                    if distribution_schedule.get("hour") is not None:
                        # For time-based scheduling, look back to the last scheduled time
                        seconds_to_look_back = calculate_lookback_seconds(
                            current_time, distribution_schedule
                        )
                    else:
                        seconds_to_look_back = distribution_schedule.get(
                            "frequency_secs", DEFAULT_LOOKBACK_SECONDS
                        )

                    blocks_to_lookback = (
                        int(seconds_to_look_back) // SECONDS_PER_BT_BLOCK
                    )
                    start_block = max(1, current_block - blocks_to_lookback)

                    (
                        sufficient_scores,
                        record_count,
                    ) = await check_sufficient_scores_for_distribution(
                        db_path, start_block, min_score_records=MIN_SCORE_RECORDS
                    )

                    if sufficient_scores:
                        logger.info(
                            f"Queueing distribution with {record_count} score records since block {start_block}"
                        )
                        asyncio.create_task(task(*task_args, **task_kwargs))
                        # After queueing, wait before checking again
                        last_check_time = current_time_ts
                        await asyncio.sleep(
                            max(
                                CHECK_INTERVAL_SECONDS,
                                frequency_secs * FREQUENCY_PERCENTAGE_DISTRIBUTION,
                            )
                        )
                    else:
                        logger.info(
                            f"Insufficient score data for distribution ({record_count} records since block {start_block})"
                        )
                        last_check_time = current_time_ts
                        await asyncio.sleep(check_interval)

                elif next_queue_time:
                    # Calculate how long to wait until next distribution
                    wait_seconds = (next_queue_time - current_time).total_seconds()
                    wait_seconds = max(
                        CHECK_INTERVAL_SECONDS, wait_seconds * WAIT_PERCENTAGE
                    )
                    logger.debug(
                        f"Next distribution queue at {next_queue_time} ({wait_seconds:.0f}s)"
                    )
                    last_check_time = current_time_ts
                    await asyncio.sleep(min(wait_seconds, check_interval))
                else:
                    last_check_time = current_time_ts
                    await asyncio.sleep(check_interval)
            else:
                await asyncio.sleep(PENDING_TRANSFER_WAIT_SECONDS)

        except Exception as e:
            logger.error(f"Error in intelligent distribution queueing: {e}")
            await asyncio.sleep(check_interval)


def calculate_lookback_seconds(
    current_time: datetime, distribution_schedule: dict
) -> int:
    """Calculate how many seconds to look back based on distribution schedule."""
    try:
        if any(
            distribution_schedule.get(k) is not None
            for k in ["hour", "minute", "second", "days"]
        ):
            # For time-based scheduling, calculate the appropriate lookback period
            lookback = timedelta(hours=1)  # Default to 1 hour

            if distribution_schedule.get("hour") is not None:
                lookback = timedelta(days=1)  # If hours specified, look back one day
            elif distribution_schedule.get("minute") is not None:
                lookback = timedelta(
                    hours=1
                )  # If minutes specified, look back one hour
            elif distribution_schedule.get("second") is not None:
                lookback = timedelta(
                    minutes=1
                )  # If seconds specified, look back one minute

            if distribution_schedule.get("days") is not None:
                lookback = timedelta(weeks=1)  # If days specified, look back one week

            return max(MIN_LOOKBACK_SECONDS, int(lookback.total_seconds()))
        else:
            # For frequency-based scheduling
            return distribution_schedule.get("frequency_secs", DEFAULT_LOOKBACK_SECONDS)

    except Exception as e:
        logger.error(f"Error calculating lookback seconds: {e}")
        return DEFAULT_LOOKBACK_SECONDS


async def get_fees(
    web3_provider: AsyncWeb3,
    block_start: int,
    block_end: int,
    blacklist_endpoint: str | None = None,
) -> dict:
    # Get the fee growth for each LP position
    fees_in_range, _ = await get_fees_in_range(
        web3_provider=web3_provider, block_start=block_start, block_end=block_end
    )

    # get list of blacklisted token ids from the blacklist endpoint with a http client, and remove them from fees_in_range
    try:
        if blacklist_endpoint:
            async with httpx.AsyncClient() as client:
                headers = {}
                if api_key := os.getenv("API_KEY"):
                    headers["Authorization"] = f"Bearer {api_key}"
                try:
                    response = await client.get(blacklist_endpoint, headers=headers)
                    response.raise_for_status()
                    blacklist = response.json().get("claimed_token_ids", [])
                    blacklist_set = set(blacklist)
                except httpx.RequestError as e:
                    logger.error(f"Error fetching blacklist: {e}")
                    blacklist_set = set()
        else:
            blacklist_set = set()

        # log blacklist information
        if blacklist_set:
            logger.info(
                f"Using blacklist from {blacklist_endpoint} with {len(blacklist_set)} entries"
            )
            logger.debug(f"Blacklist entries: {blacklist_set}")

        # Return fees_in_range filtered by blacklist
        return {
            token_id: fees
            for token_id, fees in fees_in_range.items()
            if token_id not in blacklist_set
        }
    except Exception as e:
        logger.error("Error obtaining blacklist:")
        logger.exception(e)
        return fees_in_range  # Return unfiltered fees if blacklist fetch fails


async def record_scores_for_distribution(
    db_path: str,
    subtensor: bt.AsyncSubtensor,
    coldkey: str,
    hotkey: str,
    web3_provider: AsyncWeb3,
    fee_check_period: int,
    frequency_secs: int,
    blacklist_endpoint: str | None = None,
) -> None:
    try:
        block_end = await subtensor.get_current_block()

        # Check if we should record scores to avoid duplicates
        should_record, next_record_block = await should_record_scores(
            db_path, block_end, frequency_secs
        )

        if not should_record:
            if next_record_block:
                logger.info(
                    f"Skipping score recording - next recording at block {next_record_block}"
                )
            else:
                logger.info("Skipping score recording - too soon since last record")
            return

        block_start = block_end - (fee_check_period // SECONDS_PER_BT_BLOCK)
        logger.info(
            f"Recording scores for distribution from block {block_start} to {block_end}..."
        )

        block_start_stake = block_end - (frequency_secs // SECONDS_PER_BT_BLOCK)

        # calculate delta stake for the lp miner that will be distributing rewards
        # TODO: set this to the correct
        meta_before = await subtensor.get_metagraph_info(
            netuid=NETUID, block=block_start
        )
        subnet_hotkeys = meta_before.hotkeys
        hotkey_uid = subnet_hotkeys.index(hotkey)
        delta_stake = meta_before.emission[hotkey_uid].tao

        # log the web3 provider URL
        if web3_provider is not None:
            logger.info(f"Using Web3 provider: {web3_provider.provider.endpoint_uri}")
        else:
            logger.error("Web3 provider is not set. Cannot calculate fee growth.")
            return

        # Get the fee growth for each LP position
        fees_in_range = await get_fees(
            web3_provider=web3_provider,
            block_start=block_start,
            block_end=block_end,
            blacklist_endpoint=blacklist_endpoint,
        )

        logger.debug(f"Calculated fee growth for {len(fees_in_range)} positions")

        # Calculate L1 norm (sum of absolute values) of the raw scores
        raw_scores = {
            token_id: position.total_fees_token1_equivalent
            for token_id, position in fees_in_range.items()
        }

        l1_norm = sum(abs(score) for score in raw_scores.values())

        # Normalize by L1 norm so scores sum to 1
        normalized_scores = {
            token_id: abs(score) / l1_norm if l1_norm > 0 else 0.0
            for token_id, score in raw_scores.items()
        }

        # sort the scores by value in descending order
        normalized_scores = dict(
            sorted(normalized_scores.items(), key=lambda item: item[1], reverse=True)
        )

        json_scores = json.dumps(normalized_scores)

        # we convert this to a json-serializable format
        json_growth_info = json.dumps(
            {
                str(token_id): dataclasses.asdict(fees)
                for token_id, fees in fees_in_range.items()
            }
        )

        async with aiosqlite.connect(db_path) as db:
            await db.execute(
                """
                INSERT INTO token_id_scores (block_start, block_start_stake, block_end, delta_stake, growth_info, scores)
                VALUES (?, ?, ?, ?, json(?), json(?))
                """,
                (
                    block_start,
                    block_start_stake,
                    block_end,
                    delta_stake,
                    json_growth_info,
                    json_scores,
                ),
            )
            await db.commit()

        # log the array of token ids in a json file
        token_ids = list(normalized_scores.keys())
        token_ids_file = os.path.join(os.path.dirname(db_path), TOKEN_IDS_FILE)
        with open(token_ids_file, "w") as f:
            json.dump(token_ids, f)

        logger.info(f"Successfully recorded scores for block {block_end}")

    except Exception as e:
        logger.error("Error recording scores for distribution:")
        logger.exception(e)
        raise


async def calculate_reward_distribution(
    db_path: str, start_block: int
) -> tuple[dict[str, bt.Balance], list[int]]:
    """
    Calculate distribution information from the database.
    This is done by adding up the delta scores and the scores for the owners of the token ids
    And then normalzing the sum to determine how to distribute the rewards (sum of delta stake).
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            async with db.execute(
                "SELECT id, block_start_stake, delta_stake, growth_info, scores FROM token_id_scores WHERE block_start_stake >= ? AND used = 0",
                (start_block,),
            ) as cursor:
                rows = await cursor.fetchall()

        if not rows:
            logger.warning("No records found in the database.")
            return {}, []

        ids = []

        # Calculate total delta stake, ensuring no overlapping entries
        # Group by block_start_stake to avoid double-counting overlapping periods
        stake_periods = {}
        for row in rows:
            (
                id,
                block_start_stake,
                delta_stake,
                growth_info,
                scores,
            ) = row
            # Use block_start_stake as the key to group entries by their stake calculation period
            if block_start_stake not in stake_periods:
                stake_periods[block_start_stake] = delta_stake
            # If we already have this period, keep the one with the largest delta_stake
            else:
                stake_periods[block_start_stake] = max(
                    stake_periods[block_start_stake], delta_stake
                )
            ids.append(int(id))

        # Sum up the non-overlapping delta stakes
        total_delta_stake = sum(stake_periods.values())
        if total_delta_stake <= 0:
            logger.warning(
                "Total ∆ stake has not increased, cannot calculate distribution."
            )
            return {}, []

        # owner -> reward
        reward_distribution: dict[str, bt.Balance] = {}

        for row in rows:
            _, _, delta_stake, growth_info, scores = row
            scores = json.loads(scores)
            growth_info = json.loads(growth_info)

            for token_id, score in scores.items():
                owner = growth_info[token_id]["owner"]
                if owner not in reward_distribution:
                    reward_distribution[owner] = bt.Balance.from_tao(
                        amount=0.0, netuid=NETUID
                    )
                reward_distribution[owner] += bt.Balance.from_tao(
                    amount=score * delta_stake, netuid=NETUID
                )

        # Set rewards to zero if they are less than MIN_REWARD_THRESHOLD
        # This ensures that we do not distribute very small rewards
        # which could lead to high transaction costs relative to the reward amount
        reward_distribution = {
            owner: reward if reward.tao >= MIN_REWARD_THRESHOLD else 0.0
            for owner, reward in reward_distribution.items()
        }

        # sort the distribution by the reward amount in descending order
        reward_distribution = dict(
            sorted(reward_distribution.items(), key=lambda item: item[1], reverse=True)
        )

        return reward_distribution, ids

    except Exception as e:
        logger.error(f"Error calculating reward distribution: {e}")
        raise


async def queue_distribution(
    distribution_subtensor: bt.AsyncSubtensor,
    pos_chain_subtensor: bt.AsyncSubtensor,
    wallet: bt.Wallet,
    origin_hotkey: str,
    destination_hotkey: str,
    distribution_schedule: dict,
    db_path: str = ":memory:",
) -> None:
    """
    Queue rewards distribution to LPs based on the fee growth of their positions.
    This function calculates the fee growth for each position and queues rewards accordingly.
    """
    try:
        logger.info("Starting rewards distribution queueing...")

        current_time = datetime.now(
            ZoneInfo(distribution_schedule.get("timezone", "UTC"))
        )
        current_block = await pos_chain_subtensor.get_current_block()
        logger.info(f"Current block number: {current_block}")

        # Calculate the lookback period based on the scheduling configuration
        lookback_seconds = calculate_lookback_seconds(
            current_time, distribution_schedule
        )
        blocks_to_lookback = int(lookback_seconds) // SECONDS_PER_BT_BLOCK
        start_block = max(1, current_block - blocks_to_lookback)

        logger.info(
            f"Calculating rewards distribution starting from block {start_block} "
            f"(looking back {lookback_seconds} seconds / {blocks_to_lookback} blocks)"
        )

        # Calculate the reward distribution
        reward_distribution, ids_to_update = await calculate_reward_distribution(
            db_path, start_block
        )

        # update the status of the token_id_scores in the database
        async with aiosqlite.connect(db_path) as db:
            for token_id in ids_to_update:
                await db.execute(
                    "UPDATE token_id_scores SET used = 1 WHERE id = ?",
                    (token_id,),
                )
            await db.commit()

        if not reward_distribution:
            logger.info("No rewards to distribute, skipping distribution.")
            return

        logger.info(
            f"Calculated reward distribution for {len(reward_distribution)} LPs: {reward_distribution}"
        )

        queued_distributions = 0
        failed_distributions = 0

        for owner_h160, reward in reward_distribution.items():
            if reward > 0:
                try:
                    # Convert H160 address to SS58 format for Bittensor transfer
                    owner_ss58 = h160_to_ss58(owner_h160)

                    logger.info(
                        f"Queuing distribution: {wallet.coldkeypub.ss58_address} --- {reward} ---> {owner_ss58} (H160: {owner_h160})"
                    )

                    # queue the transfer in the database
                    async with aiosqlite.connect(db_path) as db:
                        await db.execute(
                            """
                            INSERT INTO transfers (status, origin_coldkey, destination_coldkey, destination_h160, origin_hotkey, destination_hotkey, amount)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                "pending",
                                wallet.coldkeypub.ss58_address,
                                owner_ss58,
                                owner_h160,
                                origin_hotkey,
                                origin_hotkey,  # TODO: for now we transfer to the same hotkey, but we'd may want to move stake to a different hotkey in the future
                                reward.tao,
                            ),
                        )
                        await db.commit()

                    queued_distributions += 1

                except ValueError as e:
                    logger.error(f"Failed to convert address {owner_h160}: {e}")
                    failed_distributions += 1
                    continue
                except Exception as e:
                    logger.error(f"Failed to distribute reward to {owner_h160}: {e}")
                    failed_distributions += 1
                    continue
            else:
                logger.debug(f"Skipping zero reward for LP {owner_h160}")

        # Log distribution summary
        total_attempts = queued_distributions + failed_distributions
        if total_attempts > 0:
            logger.info(
                f"Reward distribution transactions: {queued_distributions} queued, {failed_distributions} failed"
            )
            if failed_distributions > 0:
                logger.warning(
                    f"{failed_distributions} distributions failed - check logs for details"
                )
        else:
            logger.info("No rewards to distribute, skipping queueing distribution.")

        logger.warning("Reward distribution logic not yet fully implemented")

    except Exception as e:
        logger.error("Error queueing rewards distribution to LPs")
        logger.exception(e)
        raise


async def run_pending_transfers(db_path: str, wallet: bt.Wallet):
    """Initiate transfers of rewards to LPs based on the queued transfers in the database."""
    try:
        async with aiosqlite.connect(db_path) as db:
            # Fetch all pending transfers from the database, and where origin_coldkey matches the wallet's coldkey
            async with db.execute(
                "SELECT id, origin_coldkey, destination_coldkey, destination_h160, origin_hotkey, destination_hotkey, amount FROM transfers WHERE status = 'pending' AND origin_coldkey = ?",
                (wallet.coldkeypub.ss58_address,),
            ) as cursor:
                rows = await cursor.fetchall()

        if not rows:
            logger.info("No pending transfers found in the database.")
            return

        async def process_single_transfer(row):
            (
                transfer_id,
                origin_coldkey,
                destination_coldkey,
                destination_h160,
                origin_hotkey,
                destination_hotkey,
                amount_tao,
            ) = row
            amount = bt.Balance.from_tao(amount_tao, netuid=NETUID)
            try:
                # TODO: implement moving stake
                # if the destination hotkey is different from the origin hotkey, then we should move stake before transferring alpha to the destination coldkey
                # if destination_hotkey != origin_hotkey:
                #     logger.info(
                #         f"Moving stake from {origin_hotkey} to {destination_hotkey} before transfers"
                #     )
                #     await distribution_subtensor.transfer_stake(
                #         wallet=wallet,
                #         destination_coldkey_ss58=destination_coldkey,
                #         hotkey_ss58=origin_hotkey,
                #         amount=amount,
                #         origin_netuid=NETUID,
                #         destination_netuid=NETUID,
                #     )
                # Transfer the alpha to the destination coldkey
                logger.info(
                    f"Transferring: {origin_coldkey} --- {amount} ---> {destination_coldkey} (H160: {destination_h160})"
                )
                # move_success = await distribution_subtensor.move_stake(
                #     wallet=wallet,
                #     origin_hotkey=origin_hotkey,
                #     destination_hotkey=destination_hotkey,
                #     origin_netuid=NETUID,
                #     destination_netuid=NETUID,
                #     amount=amount,
                # )

                # if not move_success:
                #     # raise an exception if the transfer failed
                #     raise Exception(
                #         f"Transfer failed for {origin_coldkey} to {destination_coldkey}"
                #     )

                # Transfer the stake to the destination coldkey
                transfer_success = await distribution_subtensor.transfer_stake(
                    wallet=wallet,
                    destination_coldkey_ss58=origin_coldkey,
                    hotkey_ss58=origin_hotkey,
                    amount=amount,
                    origin_netuid=NETUID,
                    destination_netuid=NETUID,
                )

                if not transfer_success:
                    # raise an exception if the transfer failed
                    raise Exception(
                        f"Transfer failed for {origin_coldkey} to {destination_coldkey}"
                    )
                logger.info(
                    f"Successfully transferred: {origin_coldkey} --- {amount} ---> {destination_coldkey} (H160: {destination_h160})"
                )

                # Update the transfer status to completed
                async with aiosqlite.connect(db_path) as db:
                    await db.execute(
                        "UPDATE transfers SET status = 'completed' WHERE id = ?",
                        (transfer_id,),
                    )
                    await db.commit()

            except Exception as e:
                logger.error(f"Failed to complete transfer {transfer_id}: {e}")
                # Update the transfer status to failed
                async with aiosqlite.connect(db_path) as db:
                    await db.execute(
                        "UPDATE transfers SET status = 'failed' WHERE id = ?",
                        (transfer_id,),
                    )
                    await db.commit()

        # Process all transfers concurrently
        await asyncio.gather(*[process_single_transfer(row) for row in rows])

    except Exception as e:
        logger.error("Error running transfers")
        logger.exception(e)
        raise


async def retry_failed_transfers(db_path: str, wallet: bt.Wallet):
    """Retry failed transfers from the database."""
    try:
        async with aiosqlite.connect(db_path) as db:
            async with db.execute(
                "SELECT id, origin_coldkey, destination_coldkey, destination_h160, origin_hotkey, destination_hotkey, amount FROM transfers WHERE status = 'failed'"
            ) as cursor:
                rows = await cursor.fetchall()

        if not rows:
            logger.info("No failed transfers found in the database.")
            return

        async def retry_single_transfer(row):
            (
                transfer_id,
                origin_coldkey,
                destination_coldkey,
                destination_h160,
                origin_hotkey,
                destination_hotkey,
                amount,
            ) = row
            try:
                # Here we would implement the actual retry logic
                # For now, we just log the action
                amount_alpha = bt.Balance.from_tao(amount, netuid=NETUID)
                logger.info(
                    f"Retrying transfer: {origin_coldkey} --- {amount_alpha} ---> {destination_coldkey} (H160: {destination_h160})"
                )

                # TODO: Here we would implement the actual transfer logic
                # For now, we just log the action with the converted address
                transfer_success = await distribution_subtensor.transfer_stake(
                    wallet=wallet,
                    destination_coldkey_ss58=destination_coldkey,
                    hotkey_ss58=origin_hotkey,
                    amount=amount_alpha,
                    origin_netuid=NETUID,
                    destination_netuid=NETUID,
                )

                if not transfer_success:
                    # raise an exception if the transfer failed
                    raise Exception(
                        f"Retry transfer failed for {origin_coldkey} to {destination_coldkey}"
                    )

                logger.info(
                    f"Successfully retried transfer: {origin_coldkey} --- {amount_alpha} ---> {destination_coldkey} (H160: {destination_h160})"
                )

                # Update the transfer status to completed
                async with aiosqlite.connect(db_path) as db:
                    await db.execute(
                        "UPDATE transfers SET status = 'completed' WHERE id = ?",
                        (transfer_id,),
                    )
                    await db.commit()

            except Exception as e:
                logger.error(f"Failed to retry transfer {transfer_id}: {e}")
                # Update the transfer status to failed again
                async with aiosqlite.connect(db_path) as db:
                    await db.execute(
                        "UPDATE transfers SET status = 'failed' WHERE id = ?",
                        (transfer_id,),
                    )
                    await db.commit()

        # Process all transfers concurrently
        await asyncio.gather(*[retry_single_transfer(row) for row in rows])

    except Exception as e:
        logger.error("Error retrying failed distributions")
        logger.exception(e)
        raise


if __name__ == "__main__":
    # Run the task in an event loop
    parser = argparse.ArgumentParser()
    add_args(parser)
    # Parse command line arguments
    bt.subtensor.add_args(parser)
    bt.wallet.add_args(parser)
    args = parser.parse_args()

    # Setup logging first
    setup_logging(args.log_dir, args.log_level, args.log_rotation, args.log_retention)

    logger.info("Starting LP Miner Distributor")
    logger.debug(f"Arguments: {vars(args)}")

    conf = bt.config(parser=parser)
    # wallet to distribute rewards from on the distribution chain
    distribution_wallet = bt.wallet(config=conf)
    # unlock the coldkey of the distribution wallet
    distribution_wallet.unlock_coldkey()
    distribution_wallet.unlock_hotkey()
    # coldkey and hotkey pair to track stake from and calculate rewards
    coldkey = (
        args.stake_coldkey
        if args.stake_coldkey
        else distribution_wallet.coldkeypub.ss58_address
    )
    hotkey = (
        args.stake_hotkey
        if args.stake_hotkey
        else distribution_wallet.hotkey.ss58_address
    )

    origin_hotkey = (
        args.origin_hotkey
        if args.origin_hotkey
        else distribution_wallet.hotkey.ss58_address
    )
    destination_hotkey = (
        args.destination_hotkey
        if args.destination_hotkey
        else distribution_wallet.hotkey.ss58_address
    )

    pos_chain_url = os.getenv("POSITION_CHAIN_PROVIDER_URL")
    distribution_subtensor = bt.AsyncSubtensor(config=conf)
    pos_chain_subtensor = bt.AsyncSubtensor(config=conf, network=pos_chain_url)

    w3 = (
        AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(pos_chain_url)) if pos_chain_url else None
    )

    if not w3:
        logger.error(
            "POSITION_CHAIN_PROVIDER_URL is not set or invalid. Cannot connect to Web3."
        )
        sys.exit(1)

    # distribution_schedule is a variable to help determine when we plan to distribute rewards based on the arguments provided
    # this will help us determine how far to look back in the database to sum up the scores and transfer amounts
    distribution_schedule = {
        "hour": args.distribution_schedule_hour,
        "minute": args.distribution_schedule_minute,
        "second": args.distribution_schedule_second,
        "timezone": args.distribution_schedule_timezone,
        "frequency_secs": args.distribution_frequency
        if args.distribution_schedule_hour is None
        else None,
    }

    # Parse days arguments if provided
    distribution_days = None
    if args.distribution_schedule_days:
        try:
            distribution_days = [
                int(d.strip()) for d in args.distribution_schedule_days.split(",")
            ]
            # Validate day values
            for day in distribution_days:
                if not 0 <= day <= 6:
                    raise ValueError(f"Day must be between 0-6, got {day}")
            logger.info(f"Distribution scheduled for days: {distribution_days}")
        except ValueError as e:
            logger.error(f"Error parsing distribution schedule days: {e}")
            exit(1)

    # Create async tasks for both functions
    async def main():
        # initialize the subtensors
        await distribution_subtensor.initialize()
        await pos_chain_subtensor.initialize()
        distribution_schedule = {
            "hour": args.distribution_schedule_hour,
            "minute": args.distribution_schedule_minute,
            "second": args.distribution_schedule_second,
            "timezone": args.distribution_schedule_timezone,
            "days": distribution_days,
            "frequency_secs": args.distribution_frequency,
        }

        tasks = [
            # Record scores task
            run_on_schedule(
                record_scores_for_distribution,
                task_kwargs={
                    "db_path": args.db_path,
                    "subtensor": pos_chain_subtensor,
                    "coldkey": coldkey,
                    "hotkey": hotkey,
                    "web3_provider": w3,
                    "fee_check_period": args.fee_check_period,
                    "frequency_secs": args.record_scores_frequency,
                    "blacklist_endpoint": os.getenv("BLACKLIST_ENDPOINT"),
                },
                frequency_secs=args.record_scores_frequency,
            ),
            # Queue distribution task
            run_on_schedule(
                queue_distribution,
                task_kwargs={
                    "distribution_subtensor": distribution_subtensor,
                    "pos_chain_subtensor": pos_chain_subtensor,
                    "wallet": distribution_wallet,
                    "origin_hotkey": origin_hotkey,
                    "destination_hotkey": destination_hotkey,
                    "distribution_schedule": distribution_schedule,
                    "db_path": args.db_path,
                },
                frequency_secs=args.distribution_frequency,
                timezone=args.distribution_schedule_timezone,
                hour=args.distribution_schedule_hour,
                minute=args.distribution_schedule_minute,
                second=args.distribution_schedule_second,
                days=distribution_days,
            ),
            # Run pending transfers task
            run_on_schedule(
                run_pending_transfers,
                task_kwargs={"db_path": args.db_path, "wallet": distribution_wallet},
                frequency_secs=args.pending_frequency,
            ),
            # Retry failed transfers task
            run_on_schedule(
                retry_failed_transfers,
                task_kwargs={"db_path": args.db_path, "wallet": distribution_wallet},
                frequency_secs=args.retry_frequency,
            ),
        ]

        logger.info("Starting main async tasks")
        await asyncio.gather(*tasks)

    # Print scheduling information
    frequency_info = f"Running record_scores_for_distribution every {args.record_scores_frequency} seconds..."
    logger.info(frequency_info)

    # TODO: update the logging for distribution scheduling
    # if args.distribution_schedule_hour is not None:
    #     days_str = f" on days {distribution_days}" if distribution_days else " daily"
    #     schedule_info = f"queue_distribution scheduled for {args.distribution_schedule_hour}:{args.distribution_schedule_minute}:{args.distribution_schedule_second} {args.distribution_schedule_timezone}{days_str}"
    #     logger.info(schedule_info)
    # else:
    #     frequency_info = (
    #         f"Running queue_distribution every {args.distribution_frequency} seconds..."
    #     )
    #     logger.info(frequency_info)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down gracefully...")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        raise
