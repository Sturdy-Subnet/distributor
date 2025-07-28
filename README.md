# SN10 LP Miner and Distributor

A system for mining and distributing rewards to Liquidity Providers (LPs) in SN10.

## Overview

This project consists of two main components:
1. **LP Miner**: Sends LP positions to validators on SN10
2. **Distributor**: Distributes rewards to LPs based on their fee generation performance

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

4. Set up environment variables in an `.env` file:
```bash
POSITION_CHAIN_PROVIDER_URL=<your-provider-url>
```

## Configuration

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
