# SN10 LP Miner and Distributor

A system for mining and distributing rewards to Liquidity Providers (LPs) in SN10.

## Overview

This project consists of two main components:
1. **LP Miner**: Sends LP positions to validators on SN10
2. **Distributor**: Distributes rewards the miner receives to LPs based on their fee generation performance

## Features

- Automated fee tracking and reward distribution
- Configurable distribution schedules (time-based or frequency-based)
- Robust logging system
- Database-backed transfer queue
- Retry mechanism for failed transfers
- Blacklist support for excluding specific token IDs

## Installation

1. Clone the repository:
```bash
git clone https://github.com/Sturdy-Subnet/distributor.git
cd distributor
```

2. Install uv (if not already installed):
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

3. Set up virtual env. and install dependencies:
```bash
uv sync
source .venv/bin/activate
```

4. Install and set up dbmate:
See [here](https://github.com/amacneil/dbmate?tab=readme-ov-file#installation) for installation instructions.

Once you have installed dbmate, run the following commands to set up the database:
```bash
dbmate --url "sqlite:database.db" create
dbmate --url "sqlite:database.db" up
```

5. Set up environment variables in an `.env` file:
```bash
# environment variables for running the distributor
POSITION_CHAIN_PROVIDER_URL=<your-provider-url>
# the following may require a connection to sn10 validator
BLACKLIST_ENDPOINT=<endpoint> # an endpoint which will send the token ids to blacklist from distribution
API_KEY=<api-key> # api to the endpoint

# environment variables for running the miner
UNISWAP_POS_OWNER_KEY="" # can be kept blank if the miner is whitelisted
BITTENSOR_MAINNET_PROVIDER_URL=<url>
BITTENSOR_WEB3_PROVIDER_URL=<url>
ETHEREUM_MAINNET_PROVIDER_URL=<url>
```

## Running the LP Miner
```
python distributor/miner.py --netuid NETUID --wallet.name COLDKEY-NAME --wallet.hotkey HOTKEY_NAME --validator.min_stake 40000 --subtensor.network NETWORK --wandb.off --logging.trace
```
For more information on running an LP miner, please consult the documentation [here](https://github.com/Sturdy-Subnet/sturdy-subnet/blob/main/docs/miner.md#starting-a-miner)

## Running the Distributor

The system can be configured using command-line arguments:

### Distribution Settings
- `--distribution-frequency`: Frequency of reward distributions (seconds)
- `--record-scores-frequency`: Frequency of score recording (seconds)
- `--fee-check-period`: Lookback period for fee calculations (seconds)

### Scheduling Options
- `--distribution-schedule-timezone`: Timezone for scheduled distributions
- `--distribution-schedule-hour`: Hour(s) to distribute (0-23)
- `--distribution-schedule-minute`: Minute(s) to distribute (0-59)
- `--distribution-schedule-second`: Second(s) to distribute (0-59)
- `--distribution-schedule-days`: Days to distribute (0=Monday, 6=Sunday)

### System Settings
- `--db-path`: SQLite database path
- `--log-dir`: Log directory
- `--log-level`: Logging level
- `--log-rotation`: Log rotation period
- `--log-retention`: Log retention period

### Wallet Configuration
- `--stake-coldkey`: Coldkey for tracking stake
- `--stake-hotkey`: Hotkey for tracking stake
- `--origin-hotkey`: Origin hotkey for transfers
- `--destination-hotkey`: Destination hotkey for transfers

### Additional Options
- `--blacklist-endpoint`: API endpoint for token ID blacklist
- `--pending-frequency`: Frequency to process pending transfers
- `--retry-frequency`: Frequency to retry failed transfers

## Usage

Basic usage:
```bash
python -m lp_miner.distributor \
  --distribution-frequency 86400 \
  --record-scores-frequency 4320 \
  --netuid 10 \
  --wallet.name default \
  --wallet.hotkey default
```

Scheduled distribution:
```bash
python -m lp_miner.distributor \
  --distribution-schedule-hour 0 \
  --distribution-schedule-minute 0 \
  --distribution-schedule-timezone UTC \
  --distribution-schedule-days 1,3,5 \
  --netuid 10 \
  --wallet.name default \
  --wallet.hotkey default
```
