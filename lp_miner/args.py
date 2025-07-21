import argparse


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
        default=4320,
        help="How often to record scores for distribution (in seconds). Default is 4320 Seconds (1.2 hours or 360 blocks).",
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
        default="database.db",
        help="Path to the SQLite database file."
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
