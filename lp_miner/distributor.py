import argparse
import asyncio
import dataclasses
from datetime import datetime
import json
import bittensor as bt
from typing import Callable, Optional, List
from sturdy.utils.taofi_subgraph import calculate_fee_growth
import aiosqlite
from zoneinfo import ZoneInfo
from loguru import logger
import sys
import os
import dotenv
from web3 import AsyncWeb3

# Load environment variables from .env file
dotenv.load_dotenv()

# Constants for time calculations
SECONDS_PER_HOUR = 3600  # 60 minutes * 60 seconds
SECONDS_PER_BT_BLOCK = 12  # 12 seconds per block
SECONDS_PER_DAY = 86400  # 24 hours * 60 minutes * 60 seconds
SECONDS_PER_MINUTE = 60  # 60 seconds in a minute


def add_args(parser: argparse.ArgumentParser):
    """
    Adds command line arguments for the script.
    """
    # Logging options
    parser.add_argument(
        "--log-dir",
        type=str,
        default="./logs",
        help="Directory to save log files. Default is './logs'.",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level. Default is INFO.",
    )

    parser.add_argument(
        "--log-rotation",
        type=str,
        default="1 day",
        help="Log file rotation (e.g., '1 day', '100 MB', '1 week'). Default is '1 day'.",
    )

    parser.add_argument(
        "--log-retention",
        type=str,
        default="30 days",
        help="Log file retention period (e.g., '30 days', '1 week'). Default is '30 days'.",
    )

    # Frequency for distributing rewards
    parser.add_argument(
        "--distribution-frequency",
        type=int,
        default=86400,
        help="How often to distribute rewards to LPs (in seconds). Default is 1 day (86400 seconds).",
    )

    # Frequency for recording scores
    parser.add_argument(
        "--record-scores-frequency",
        type=int,
        default=3600,
        help="How often to record scores for distribution (in seconds). Default is 1 hour (3600 seconds).",
    )

    # Scheduling options for distribution
    parser.add_argument(
        "--distribution-schedule-timezone",
        type=str,
        default="UTC",
        help="Timezone for distribution schedule (e.g., 'UTC', 'Europe/Berlin', 'America/New_York'). Default is UTC.",
    )

    parser.add_argument(
        "--distribution-schedule-hour",
        type=int,
        default=None,
        help="Hour of day to distribute rewards (0-23). If not specified, uses frequency-based scheduling.",
    )

    parser.add_argument(
        "--distribution-schedule-minute",
        type=int,
        default=0,
        help="Minute of hour to distribute rewards (0-59). Default is 0.",
    )

    parser.add_argument(
        "--distribution-schedule-second",
        type=int,
        default=0,
        help="Second of minute to distribute rewards (0-59). Default is 0.",
    )

    parser.add_argument(
        "--distribution-schedule-days",
        type=str,
        default=None,
        help="Days of week to distribute rewards (comma-separated: 0=Monday, 6=Sunday). e.g., '1' for Tuesday only, '0,2,4' for Mon/Wed/Fri.",
    )

    # Scheduling options for recording scores
    parser.add_argument(
        "--record-scores-schedule-timezone",
        type=str,
        default="UTC",
        help="Timezone for recording scores schedule. Default is UTC.",
    )

    parser.add_argument(
        "--record-scores-schedule-hour",
        type=int,
        default=None,
        help="Hour of day to record scores (0-23). If not specified, uses frequency-based scheduling.",
    )

    parser.add_argument(
        "--record-scores-schedule-minute",
        type=int,
        default=0,
        help="Minute of hour to record scores (0-59). Default is 0.",
    )

    parser.add_argument(
        "--record-scores-schedule-second",
        type=int,
        default=0,
        help="Second of minute to record scores (0-59). Default is 0.",
    )

    parser.add_argument(
        "--record-scores-schedule-days",
        type=str,
        default=None,
        help="Days of week to record scores (comma-separated: 0=Monday, 6=Sunday).",
    )

    # a parameter used to specify how far back we go to check the fees LPs have made
    parser.add_argument(
        "--fee-check-period",
        type=int,
        default=86400,
        help="How far back to check fees (in seconds). Default is 1 day (86400 seconds).",
    )

    # sqlite database path
    parser.add_argument(
        "--db-path",
        type=str,
        default=":memory:",
        help="Path to the SQLite database file. Defaults to an in-memory database.",
    )

    # blacklist endpoint
    parser.add_argument(
        "--blacklist-endpoint",
        type=str or None,
        default=None,
        help="Endpoint used to get a list of token ids to disclude from distribution.",
    )


def time_matches(
    now: datetime,
    second: Optional[int] = None,
    minute: Optional[int] = None,
    hour: Optional[int] = None,
    days: Optional[List[int]] = None,
) -> bool:
    """Check if current time matches the schedule"""
    return (
        (second is None or now.second == second)
        and (minute is None or now.minute == minute)
        and (hour is None or now.hour == hour)
        and (days is None or now.weekday() in days)
    )


def setup_logging(log_dir: str, log_level: str, log_rotation: str, log_retention: str):
    """Setup loguru logging with file output"""
    # Remove default handler
    logger.remove()

    # Add console handler with colored output
    logger.add(
        sys.stderr,
        level=log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True,
    )

    # Create log directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)

    # Add file handler
    log_file_path = os.path.join(log_dir, "distributor_{time:YYYY-MM-DD}.log")
    logger.add(
        log_file_path,
        level=log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        rotation=log_rotation,
        retention=log_retention,
        compression="zip",
    )

    logger.info(f"Logging initialized - Level: {log_level}, Log dir: {log_dir}")


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

    If hour is specified, uses time-based scheduling, otherwise uses frequency-based scheduling.

    - task_args: tuple of positional arguments to pass to the task
    - task_kwargs: dict of keyword arguments to pass to the task
    - frequency_secs: how often to run the task in seconds (used when hour is None)
    - timezone: timezone for time-based scheduling (e.g., 'UTC', 'Europe/Berlin')
    - hour: hour of day to run the task (0-23, enables time-based scheduling)
    - minute: minute of hour to run the task (0-59, default 0)
    - second: second of minute to run the task (0-59, default 0)
    - days: list of weekdays to run on (0=Monday, 6=Sunday, None=every day)
    """
    if task_kwargs is None:
        task_kwargs = {}

    # Time-based scheduling
    if hour is not None:
        last_run_date = None
        logger.info(
            f"Starting time-based scheduling for {task.__name__} at {hour:02d}:{minute:02d}:{second:02d} {timezone}"
        )

        while True:
            try:
                # Get current time in the specified timezone
                tz = ZoneInfo(timezone)
                now = datetime.now(tz)

                # Check if current time matches the schedule
                if time_matches(now, second, minute, hour, days):
                    # Ensure we only run once per matching time period
                    current_date = now.date()
                    if last_run_date != current_date or (
                        days is not None and now.weekday() not in days
                    ):
                        if days is None or now.weekday() in days:
                            logger.info(
                                f"Running scheduled task {task.__name__} at {now.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                            )
                            asyncio.create_task(task(*task_args, **task_kwargs))
                            last_run_date = current_date

                await asyncio.sleep(1)  # Check every second

            except Exception as e:
                logger.error(
                    f"Invalid timezone '{timezone}': {e}. Falling back to UTC."
                )
                timezone = "UTC"

    # Frequency-based scheduling (legacy behavior)
    else:
        if frequency_secs is None:
            raise ValueError(
                "Either frequency_secs must be specified or hour must be provided for time-based scheduling"
            )

        logger.info(
            f"Starting frequency-based scheduling for {task.__name__} every {frequency_secs} seconds"
        )
        last_run_time = 0
        while True:
            current_time = datetime.now().timestamp()

            if current_time - last_run_time >= frequency_secs:
                logger.debug(f"Running scheduled task {task.__name__}")
                asyncio.create_task(task(*task_args, **task_kwargs))
                last_run_time = current_time

            await asyncio.sleep(1)  # Check every second


async def record_scores_for_distribution(
    db_path: str,
    subtensor: bt.AsyncSubtensor,
    web3_provider: AsyncWeb3,
    fee_check_period: int,
) -> None:
    try:
        block_end = await subtensor.get_current_block()
        block_start = block_end - (fee_check_period // SECONDS_PER_BT_BLOCK)
        logger.info(
            f"Recording scores for distribution from block {block_start} to {block_end}..."
        )

        # log the web3 provider URL
        if web3_provider is not None:
            logger.info(f"Using Web3 provider: {web3_provider.provider.endpoint_uri}")
        else:
            logger.error("Web3 provider is not set. Cannot calculate fee growth.")
            return

        # Get the fee growth for each LP position
        _, _, fee_growth_info = await calculate_fee_growth(
            web3_provider=web3_provider, block_start=block_start, block_end=block_end
        )

        logger.debug(f"Calculated fee growth for {len(fee_growth_info)} positions")
        logger.debug(f"Fee growth info: {fee_growth_info[237]}")

        # score the token ids based on their fee growth
        # add up the total_fees_token1_equivalent in the fee_growth_info for each token_id,
        # normalize it, and store it in a dict
        total_fees = sum(
            [
                position.total_fees_token1_equivalent
                for position in fee_growth_info.values()
            ]
        )

        normalized_scores = {
            token_id: position.total_fees_token1_equivalent / total_fees
            if total_fees > 0
            else 0.0
            for token_id, position in fee_growth_info.items()
        }
        json_scores = json.dumps(normalized_scores)

        # fee_growth_info is a dict[int, PositionFees]
        # we convert this to a json-serializable format
        json_growth_info = json.dumps(
            {
                str(token_id): dataclasses.asdict(fees)
                for token_id, fees in fee_growth_info.items()
            }
        )

        async with aiosqlite.connect(db_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS token_id_scores (
                    block_end INTEGER,
                    growth_info TEXT,
                    scores TEXT
                )
                """
            )
            await db.execute(
                """
                INSERT INTO token_id_scores (block_end, growth_info, scores)
                VALUES (?, json(?), json(?))
                """,
                (block_end, json_growth_info, json_scores),
            )
            await db.commit()

        logger.info(f"Successfully recorded scores for block {block_end}")

    except Exception as e:
        logger.error(f"Error recording scores for distribution: {e}")
        raise


async def distribute_rewards_task_to_lps(subtensor: bt.AsyncSubtensor) -> None:
    """
    Distribute rewards to LPs based on the fee growth of their positions.
    This function calculates the fee growth for each position and distributes rewards accordingly.
    """
    try:
        logger.info("Starting reward distribution to LPs...")
        # TODO: Implement the actual distribution logic
        logger.warning("Reward distribution logic not yet implemented")

    except Exception as e:
        logger.error(f"Error distributing rewards to LPs: {e}")
        raise


if __name__ == "__main__":
    # Run the task in an event loop
    parser = argparse.ArgumentParser()
    add_args(parser)
    # Parse command line arguments
    bt.subtensor.add_args(parser)
    args = parser.parse_args()

    # Setup logging first
    setup_logging(args.log_dir, args.log_level, args.log_rotation, args.log_retention)

    logger.info("Starting LP Miner Distributor")
    logger.debug(f"Arguments: {vars(args)}")

    conf = bt.config(parser=parser)
    subtensor = bt.AsyncSubtensor(config=conf)
    w3_url = os.getenv("WEB3_PROVIDER_URL")
    w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(w3_url)) if w3_url else None
    if not w3:
        logger.error("WEB3_PROVIDER_URL is not set or invalid. Cannot connect to Web3.")
        sys.exit(1)

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

    record_scores_days = None
    if args.record_scores_schedule_days:
        try:
            record_scores_days = [
                int(d.strip()) for d in args.record_scores_schedule_days.split(",")
            ]
            # Validate day values
            for day in record_scores_days:
                if not 0 <= day <= 6:
                    raise ValueError(f"Day must be between 0-6, got {day}")
            logger.info(f"Score recording scheduled for days: {record_scores_days}")
        except ValueError as e:
            logger.error(f"Error parsing record scores schedule days: {e}")
            exit(1)

    # Create async tasks for both functions
    async def main():
        try:
            logger.info("Starting main async tasks")

            # Create tasks for both functions to run concurrently
            distribute_task = asyncio.create_task(
                run_on_schedule(
                    task=distribute_rewards_task_to_lps,
                    task_kwargs={"subtensor": subtensor},
                    frequency_secs=args.distribution_frequency
                    if args.distribution_schedule_hour is None
                    else None,
                    timezone=args.distribution_schedule_timezone,
                    hour=args.distribution_schedule_hour,
                    minute=args.distribution_schedule_minute,
                    second=args.distribution_schedule_second,
                    days=distribution_days,
                )
            )

            record_scores_task = asyncio.create_task(
                run_on_schedule(
                    task=record_scores_for_distribution,
                    task_kwargs={
                        "db_path": args.db_path,
                        "subtensor": subtensor,
                        "web3_provider": w3,
                        "fee_check_period": args.fee_check_period,
                    },
                    frequency_secs=args.record_scores_frequency
                    if args.record_scores_schedule_hour is None
                    else None,
                    timezone=args.record_scores_schedule_timezone,
                    hour=args.record_scores_schedule_hour,
                    minute=args.record_scores_schedule_minute,
                    second=args.record_scores_schedule_second,
                    days=record_scores_days,
                )
            )

            # Wait for both tasks to complete (they run indefinitely)
            await asyncio.gather(distribute_task, record_scores_task)

        except Exception as e:
            logger.error(f"Error in main async function: {e}")
            raise

    # Print scheduling information
    if args.distribution_schedule_hour is not None:
        days_str = f" on days {distribution_days}" if distribution_days else " daily"
        schedule_info = f"Distribution scheduled for {args.distribution_schedule_hour:02d}:{args.distribution_schedule_minute:02d}:{args.distribution_schedule_second:02d} {args.distribution_schedule_timezone}{days_str}"
        logger.info(schedule_info)
    else:
        frequency_info = f"Running distribute_rewards_task_to_lps every {args.distribution_frequency} seconds..."
        logger.info(frequency_info)

    if args.record_scores_schedule_hour is not None:
        days_str = f" on days {record_scores_days}" if record_scores_days else " daily"
        schedule_info = f"Score recording scheduled for {args.record_scores_schedule_hour:02d}:{args.record_scores_schedule_minute:02d}:{args.record_scores_schedule_second:02d} {args.record_scores_schedule_timezone}{days_str}"
        logger.info(schedule_info)
    else:
        frequency_info = f"Running record_scores_for_distribution every {args.record_scores_frequency} seconds..."
        logger.info(frequency_info)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down gracefully...")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        raise
