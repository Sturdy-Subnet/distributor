# Reward Distribution Scheduling Examples

The updated distributor script now supports flexible scheduling for both reward distribution and score recording. You can use either frequency-based scheduling (the original behavior) or time-based scheduling with specific times and days.

## Time-based Scheduling Examples

### Every Tuesday at 12:00 AM UTC
```bash
python lp_miner/distributor.py \
    --distribution-schedule-hour 0 \
    --distribution-schedule-minute 0 \
    --distribution-schedule-second 0 \
    --distribution-schedule-days "1" \
    --distribution-schedule-timezone "UTC"
```

### Every day at 12:00 AM CET (Central European Time)
```bash
python lp_miner/distributor.py \
    --distribution-schedule-hour 0 \
    --distribution-schedule-minute 0 \
    --distribution-schedule-second 0 \
    --distribution-schedule-timezone "Europe/Berlin"
```

### Every hour at 30 minutes past the hour (e.g., 1:30, 2:30, 3:30, etc.)
```bash
python lp_miner/distributor.py \
    --distribution-schedule-minute 30 \
    --distribution-schedule-second 0 \
    --distribution-schedule-timezone "UTC"
```

### Monday, Wednesday, Friday at 6:00 PM EST
```bash
python lp_miner/distributor.py \
    --distribution-schedule-hour 18 \
    --distribution-schedule-minute 0 \
    --distribution-schedule-second 0 \
    --distribution-schedule-days "0,2,4" \
    --distribution-schedule-timezone "America/New_York"
```

### Different schedules for distribution vs score recording
```bash
python lp_miner/distributor.py \
    --distribution-schedule-hour 0 \
    --distribution-schedule-minute 0 \
    --distribution-schedule-timezone "UTC" \
    --distribution-schedule-days "1" \
    --record-scores-schedule-hour 12 \
    --record-scores-schedule-minute 0 \
    --record-scores-schedule-timezone "UTC"
```

## Frequency-based Scheduling (Original Behavior)

If you don't specify `--distribution-schedule-hour` or `--record-scores-schedule-hour`, the script will use the original frequency-based scheduling:

```bash
python lp_miner/distributor.py \
    --distribution-frequency 86400 \
    --record-scores-frequency 3600
```

## Important Notes

- **Day numbers**: 0=Monday, 1=Tuesday, 2=Wednesday, 3=Thursday, 4=Friday, 5=Saturday, 6=Sunday
- **Timezone formats**: Use standard timezone names like "UTC", "Europe/Berlin", "America/New_York", "Asia/Tokyo", etc.
- **Time validation**: Hours must be 0-23, minutes and seconds must be 0-59
- **Mixed scheduling**: You can use time-based scheduling for one task and frequency-based for the other
- **Timezone fallback**: If an invalid timezone is specified, the script will fall back to UTC

## Common Timezone Examples

- UTC: `"UTC"`
- Eastern Time: `"America/New_York"`
- Central European Time: `"Europe/Berlin"`
- Pacific Time: `"America/Los_Angeles"`
- Japanese Time: `"Asia/Tokyo"`
- London Time: `"Europe/London"`

## Default Values

- Timezone: UTC
- Minute: 0
- Second: 0
- Days: None (every day)
