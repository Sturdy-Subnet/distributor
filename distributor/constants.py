# Constants for time calculations
SECONDS_PER_HOUR = 3600  # 60 minutes * 60 seconds
SECONDS_PER_BT_BLOCK = 12  # 12 seconds per block
BLOCKS_PER_EPOCH = 360
SECONDS_PER_DAY = 86400  # 24 hours * 60 minutes * 60 seconds
SECONDS_PER_MINUTE = 60  # 60 seconds in a minute
NETUID = 2  # netuid
MIN_REWARD_THRESHOLD = (
    1e-4  # minimum reward for an LP position to be considered for distribution
)

# New constants for distributor
ALPHA_PER_EPOCH = 360  # 360 alpha gets emitted per epoch to the subnet
MINER_EMISSION_PCT = 0.41  # 41% miners receive 41% of subnet emissions on SN10

CHECK_INTERVAL_SECONDS = 300  # 5 minutes in seconds
SCORE_CHECK_INTERVAL_SECONDS = 60  # Check blockchain state every minute
PENDING_TRANSFER_WAIT_SECONDS = 300  # Check every 300 seconds when not in check window
MIN_QUEUE_DISTRIBUTION_GAP = 3600  # At least 1 hour gap between queuing distributions
MIN_LOOKBACK_SECONDS = 30  # At least 30 seconds lookback for distributions
DEFAULT_LOOKBACK_SECONDS = 86400  # Default 24 hours lookback
WAIT_PERCENTAGE = 0.9  # Wait 90% of the time before next check
FREQUENCY_PERCENTAGE_SCORE = 0.1  # Wait 10% of frequency for score recording
FREQUENCY_PERCENTAGE_DISTRIBUTION = 0.25  # Wait 25% of frequency for distribution
MIN_SCORE_RECORDS = 1  # Minimum score records required for distribution

# Default argument values
DEFAULT_DISTRIBUTION_FREQUENCY = 86400  # 1 day in seconds
DEFAULT_RECORD_SCORES_FREQUENCY = 4320  # 1.2 hours or 360 blocks in seconds
DEFAULT_PENDING_FREQUENCY = 3600  # 1 hour in seconds
DEFAULT_RETRY_FREQUENCY = 3600  # 1 hour in seconds
DEFAULT_FEE_CHECK_PERIOD = 86400  # 1 day in seconds
DEFAULT_DB_PATH = "database.db"  # Default database file path
DEFAULT_LOG_DIR = "./logs"  # Default log directory
DEFAULT_LOG_LEVEL = "INFO"  # Default log level
DEFAULT_LOG_ROTATION = "1 day"  # Default log rotation period
DEFAULT_LOG_RETENTION = "30 days"  # Default log retention period
DEFAULT_TIMEZONE = "UTC"  # Default timezone for scheduling
TOKEN_IDS_FILE = "token_ids.json"  # Default file for storing token IDs

# Transaction processing constants
TRANSACTION_DELAY_SECONDS = 2  # Delay between transactions to avoid rate limiting
