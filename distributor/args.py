import argparse
from constants import (
    DEFAULT_DISTRIBUTION_FREQUENCY,
    DEFAULT_RECORD_SCORES_FREQUENCY,
    DEFAULT_PENDING_FREQUENCY,
    DEFAULT_RETRY_FREQUENCY,
    DEFAULT_FEE_CHECK_PERIOD,
    DEFAULT_DB_PATH,
    DEFAULT_LOG_DIR,
    DEFAULT_LOG_LEVEL,
    DEFAULT_LOG_ROTATION,
    DEFAULT_LOG_RETENTION,
    DEFAULT_TIMEZONE,
)


def add_args(parser: argparse.ArgumentParser):
    """
    Adds command line arguments for the script.
    """
    # Logging options
    parser.add_argument(
        "--log-dir",
        type=str,
        default=DEFAULT_LOG_DIR,
        help="Directory to save log files. Default is './logs'.",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default=DEFAULT_LOG_LEVEL,
        choices=["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level. Default is INFO.",
    )

    parser.add_argument(
        "--log-rotation",
        type=str,
        default=DEFAULT_LOG_ROTATION,
        help="Log file rotation (e.g., '1 day', '100 MB', '1 week'). Default is '1 day'.",
    )

    parser.add_argument(
        "--log-retention",
        type=str,
        default=DEFAULT_LOG_RETENTION,
        help="Log file retention period (e.g., '30 days', '1 week'). Default is '30 days'.",
    )

    # Frequency for distributing rewards
    parser.add_argument(
        "--distribution-frequency",
        type=int,
        default=DEFAULT_DISTRIBUTION_FREQUENCY,
        help="How often to distribute rewards to LPs (in seconds). Default is 1 day (86400 seconds).",
    )

    # Frequency for recording scores
    parser.add_argument(
        "--record-scores-frequency",
        type=int,
        default=DEFAULT_RECORD_SCORES_FREQUENCY,
        help="How often to record scores for distribution (in seconds). Default is 4320 Seconds (1.2 hours or 360 blocks).",
    )

    # Scheduling options for distribution
    parser.add_argument(
        "--distribution-schedule-timezone",
        type=str,
        default=DEFAULT_TIMEZONE,
        help="Timezone for distribution schedule (e.g., 'UTC', 'Europe/Berlin', 'America/New_York'). Default is UTC.",
    )

    parser.add_argument(
        "--distribution-schedule-hour",
        type=int,
        default=None,
        help="Hour(s) to distribute rewards (0-23). Run every hour if not specified.",
    )

    parser.add_argument(
        "--distribution-schedule-minute",
        type=int,
        default=None,
        help="Minute(s) to distribute rewards (0-59). Run every specified minute of matching hours.",
    )

    parser.add_argument(
        "--distribution-schedule-second",
        type=int,
        default=None,
        help="Second(s) to distribute rewards (0-59). Run every specified second of matching minutes.",
    )

    parser.add_argument(
        "--distribution-schedule-days",
        type=str,
        default=None,
        help="Days of week to distribute rewards (comma-separated: 0=Monday, 6=Sunday). e.g., '1' for Tuesday only, '0,2,4' for Mon/Wed/Fri.",
    )

    # Frequency for running pending transfers
    parser.add_argument(
        "--pending-frequency",
        type=int,
        default=DEFAULT_PENDING_FREQUENCY,
        help="How often to run pending transfers (in seconds). Default is 1 hour (3600 seconds).",
    )

    # Frequency for retrying failed transfers
    parser.add_argument(
        "--retry-frequency",
        type=int,
        default=DEFAULT_RETRY_FREQUENCY,
        help="How often to retry failed transfers (in seconds). Default is 1 hour (3600 seconds).",
    )

    # a parameter used to specify how far back we go to check the fees LPs have made
    parser.add_argument(
        "--fee-check-period",
        type=int,
        default=DEFAULT_FEE_CHECK_PERIOD,
        help="How far back to check fees (in seconds). Default is 1 day (86400 seconds).",
    )

    # sqlite database path
    parser.add_argument(
        "--db-path",
        type=str,
        default=DEFAULT_DB_PATH,
        help="Path to the SQLite database file.",
    )

    # coldkey to track stake from and calculate rewards
    parser.add_argument(
        "--stake-coldkey",
        type=str,
        default=None,
        help="Coldkey to track stake from and calculate rewards. If not provided, will use the coldkey from the wallet.",
    )

    # hotkey to track stake from and calculate rewards
    parser.add_argument(
        "--stake-hotkey",
        type=str,
        default=None,
        help="Hotkey to track stake from and calculate rewards. If not provided, will use the hotkey from the wallet.",
    )

    # origin hotkey to send stake from
    parser.add_argument(
        "--origin-hotkey",
        type=str,
        default=None,
        help="Origin hotkey to send stake from. If not provided, will use the hotkey from the wallet.",
    )

    # destination hotkey to send stake to
    parser.add_argument(
        "--destination-hotkey",
        type=str,
        default=None,
        help="Destination hotkey to send stake to. If not provided, will use the hotkey from the wallet.",
    )

    # blacklist endpoint
    parser.add_argument(
        "--blacklist-endpoint",
        type=str or None,
        default=None,
        help="Endpoint used to get a list of token ids to disclude from distribution.",
    )
