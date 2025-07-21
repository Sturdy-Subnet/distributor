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
from args import add_args
from constants import NETUID, SECONDS_PER_BT_BLOCK, MIN_REWARD_THRESHOLD
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
    coldkey: str,
    hotkey: str,
    web3_provider: AsyncWeb3,
    fee_check_period: int,
    frequency_secs: int,
) -> None:
    try:
        block_end = await subtensor.get_current_block()
        block_start = block_end - (fee_check_period // SECONDS_PER_BT_BLOCK)
        logger.info(
            f"Recording scores for distribution from block {block_start} to {block_end}..."
        )

        block_start_stake = block_end - (frequency_secs // SECONDS_PER_BT_BLOCK)

        # calculate delta stake for the lp miner that will be distributing rewards
        stake_before = await subtensor.get_stake(
            coldkey_ss58=coldkey,
            hotkey_ss58=hotkey,
            netuid=NETUID,
            block=block_start_stake,
        )
        stake_after = await subtensor.get_stake(
            coldkey_ss58=coldkey,
            hotkey_ss58=hotkey,
            netuid=NETUID,
            block=block_end,
        )
        delta_stake = (stake_after - stake_before).tao

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

        # Filter out out-of-range positions before calculating total fees
        for token_id, position in fee_growth_info.items():
            # TODO: do this in sturdy subnet code, not here
            if not (
                position.tick_lower <= position.current_tick <= position.tick_upper
            ):
                position.total_fees_token1_equivalent = 0.0

        total_fees = sum(
            position.total_fees_token1_equivalent
            for position in fee_growth_info.values()
        )

        normalized_scores = {
            token_id: position.total_fees_token1_equivalent / total_fees
            if total_fees > 0
            else 0.0
            for token_id, position in fee_growth_info.items()
        }

        # sort the scores by value in descending order
        normalized_scores = dict(
            sorted(normalized_scores.items(), key=lambda item: item[1], reverse=True)
        )

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

        logger.info(f"Successfully recorded scores for block {block_end}")

    except Exception as e:
        logger.error("Error recording scores for distribution:")
        # log error with traceback
        logger.exception(e)
        raise


async def calculate_reward_distribution(
    db_path: str, start_block: int
) -> dict[str, bt.Balance]:
    """
    Calculate distribution information from the database.
    This is done by adding up the delta scores and the scores for the owners of the token ids
    And then normalzing the sum to determine how to distribute the rewards (sum of delta stake).
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            async with db.execute(
                "SELECT block_start, block_start_stake, block_end, delta_stake, growth_info, scores FROM token_id_scores WHERE block_start_stake >= ?",
                (start_block,),
            ) as cursor:
                rows = await cursor.fetchall()

        if not rows:
            logger.warning("No records found in the database.")
            return []

        # Calculate total delta stake, ensuring no overlapping entries
        # Group by block_start_stake to avoid double-counting overlapping periods
        stake_periods = {}
        for row in rows:
            (
                _,
                block_start_stake,
                block_end,
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

        # Sum up the non-overlapping delta stakes
        total_delta_stake = sum(stake_periods.values())
        if total_delta_stake <= 0:
            logger.warning(
                "Total ∆ stake has not increased, cannot calculate distribution."
            )
            return {}

        # owner -> reward
        score_distribution = {}

        for row in rows:
            block_end, delta_stake, growth_info, scores = row
            scores = json.loads(scores)
            growth_info = json.loads(growth_info)

            for token_id, score in scores.items():
                owner = growth_info[token_id]["owner"]
                if owner not in score_distribution:
                    score_distribution[owner] = 0.0
                score_distribution[owner] += score * delta_stake

        # Normalize scores by max score in score_distribution
        max_score = max(score_distribution.values())
        if max_score == 0:
            logger.warning("Max score is zero, cannot normalize distribution.")
            return {
                owner: bt.Balance.from_tao(0.0, netuid=NETUID)
                for owner in score_distribution
            }

        normalized_distribution = {
            owner: score / max_score for owner, score in score_distribution.items()
        }

        # Normalize the distribution by total delta stake to obtain reward distribution
        if total_delta_stake > 0:
            reward_distribution = {
                owner: score * total_delta_stake
                for owner, score in normalized_distribution.items()
            }
        else:
            logger.warning("Total delta stake is zero, cannot normalize distribution.")
            reward_distribution = {owner: 0.0 for owner in normalized_distribution}

        # Set rewards to zero if they are less than MIN_REWARD_THRESHOLD
        # This ensures that we do not distribute very small rewards
        # which could lead to high transaction costs relative to the reward amount
        reward_distribution = {
            owner: reward if reward >= MIN_REWARD_THRESHOLD else 0.0
            for owner, reward in reward_distribution.items()
        }

        # sort the distribution by the reward amount in descending order
        reward_distribution = dict(
            sorted(reward_distribution.items(), key=lambda item: item[1], reverse=True)
        )

        # convert amounts to Balance objects
        reward_distribution = {
            owner: bt.Balance.from_tao(amount=reward, netuid=NETUID)
            for owner, reward in reward_distribution.items()
        }

        return reward_distribution

    except Exception as e:
        logger.error(f"Error calculating reward distribution: {e}")
        raise


async def distribute_rewards_task_to_lps(
    distribution_subtensor: bt.AsyncSubtensor,
    pos_chain_subtensor: bt.AsyncSubtensor,
    wallet: bt.Wallet,
    origin_hotkey: str,
    destination_hotkey: str,
    distribution_schedule: dict,
    db_path: str = ":memory:",
) -> None:
    """
    Distribute rewards to LPs based on the fee growth of their positions.
    This function calculates the fee growth for each position and distributes rewards accordingly.
    """
    try:
        logger.info("Starting reward distribution to LPs...")

        # get the current block number
        current_block = await pos_chain_subtensor.get_current_block()
        logger.info(f"Current block number: {current_block}")
        # get the block from which we will start distributing rewards
        # we will claculate the amount of seconds to look back based on the distribution schedule
        if distribution_schedule["hour"] is not None:
            # If hour is specified, calculate the time to look back based on the schedule
            now = datetime.now(ZoneInfo(distribution_schedule["timezone"]))
            distribution_time = datetime(
                now.year,
                now.month,
                now.day,
                distribution_schedule["hour"],
                distribution_schedule["minute"],
                distribution_schedule["second"],
                tzinfo=ZoneInfo(distribution_schedule["timezone"]),
            )
            # Calculate how many seconds to look back
            seconds_to_look_back = (now - distribution_time).total_seconds()
            if seconds_to_look_back < 0:
                logger.info(
                    "Current time is before the scheduled distribution time, skipping distribution."
                )
                return
        else:
            # If hour is not specified, use the frequency_secs to determine how far back to look
            seconds_to_look_back = distribution_schedule.get("frequency_secs", 86400)

        blocks_to_lookback = int(seconds_to_look_back) // SECONDS_PER_BT_BLOCK
        current_block = await pos_chain_subtensor.get_current_block()
        start_block = max(1, current_block - blocks_to_lookback)

        # Calculate the reward distribution
        reward_distribution = await calculate_reward_distribution(db_path, start_block)

        if not reward_distribution:
            logger.info("No rewards to distribute, skipping distribution.")
            return

        logger.info(
            f"Calculated reward distribution for {len(reward_distribution)} LPs: {reward_distribution}"
        )

        successful_distributions = 0
        failed_distributions = 0

        for owner_h160, reward in reward_distribution.items():
            if reward > 0:
                try:
                    # Convert H160 address to SS58 format for Bittensor transfer
                    owner_ss58 = h160_to_ss58(owner_h160)

                    logger.info(f"Converting LP address: {owner_h160} -> {owner_ss58}")
                    logger.info(
                        f"Distributing {reward} to LP {owner_ss58} (h160: {owner_h160}) from {origin_hotkey} to {destination_hotkey}"
                    )

                    # Here we would implement the actual transfer logic
                    # For now, we just log the action with the converted address
                    # await distribution_subtensor.transfer_stake(
                    #     wallet=wallet,
                    #     hotkey_ss58=owner_ss58,
                    #     destination_ss58=destination_hotkey,
                    #     amount=reward,
                    #     netuid=NETUID,
                    # )

                    successful_distributions += 1

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
        total_attempts = successful_distributions + failed_distributions
        if total_attempts > 0:
            logger.info(
                f"Reward distribution completed: {successful_distributions} successful, {failed_distributions} failed"
            )
            if failed_distributions > 0:
                logger.warning(
                    f"{failed_distributions} distributions failed - check logs for details"
                )
        else:
            logger.info("No rewards to distribute, skipping distribution.")

        logger.warning("Reward distribution logic not yet fully implemented")

    except Exception as e:
        logger.error(f"Error distributing rewards to LPs: {e}")
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
    # coldkey and hotkey pair to track stake from and calculate rewards
    coldkey = (
        args.stake_coldkey
        if args.stake_coldkey
        else distribution_wallet.coldkeypub.ss58_address
    )
    hotkey = (
        args.stake_hotkeyS
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
        try:
            logger.info("Starting main async tasks")

            # Create tasks for both functions to run concurrently
            record_scores_task = asyncio.create_task(
                run_on_schedule(
                    task=record_scores_for_distribution,
                    task_kwargs={
                        "db_path": args.db_path,
                        "subtensor": pos_chain_subtensor,
                        "coldkey": coldkey,
                        "hotkey": hotkey,
                        "web3_provider": w3,
                        "fee_check_period": args.fee_check_period,
                        "frequency_secs": args.record_scores_frequency,
                    },
                    frequency_secs=args.record_scores_frequency,
                )
            )

            distribute_task = asyncio.create_task(
                run_on_schedule(
                    task=distribute_rewards_task_to_lps,
                    task_kwargs={
                        "distribution_subtensor": distribution_subtensor,
                        "pos_chain_subtensor": pos_chain_subtensor,
                        "wallet": distribution_wallet,
                        "origin_hotkey": origin_hotkey,
                        "destination_hotkey": destination_hotkey,
                        "db_path": args.db_path,
                        "distribution_schedule": distribution_schedule,
                    },
                    # TODO: use distribution_schedulte to determine how often to run this task instead
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
            # Wait for both tasks to complete (they run indefinitely)
            await asyncio.gather(distribute_task, record_scores_task)

        except Exception as e:
            logger.error(f"Error in main async function: {e}")
            raise

    # Print scheduling information
    frequency_info = f"Running record_scores_for_distribution every {args.record_scores_frequency} seconds..."
    logger.info(frequency_info)

    if args.distribution_schedule_hour is not None:
        days_str = f" on days {distribution_days}" if distribution_days else " daily"
        schedule_info = f"Distribution scheduled for {args.distribution_schedule_hour:02d}:{args.distribution_schedule_minute:02d}:{args.distribution_schedule_second:02d} {args.distribution_schedule_timezone}{days_str}"
        logger.info(schedule_info)
    else:
        frequency_info = f"Running distribute_rewards_task_to_lps every {args.distribution_frequency} seconds..."
        logger.info(frequency_info)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down gracefully...")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        raise
