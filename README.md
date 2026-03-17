# polymarket-btc15m-research

BTC 15m Polymarket directional market research framework — Phase 1.

This is a **research and data collection tool**, not a trading bot.
Phase 1 scope: config, structured logging, and a heartbeat loop stub.
No strategy logic, no execution, no ML/RL.

---

## Project Structure

```
PolymarketBotv3/
├── config/
│   └── settings.yaml          # all runtime config
├── src/
│   ├── config_loader.py       # loads + validates settings.yaml
│   ├── logger.py              # structured logging factory (structlog)
│   └── run.py                 # entry point
├── tests/
│   ├── test_config_loader.py
│   └── test_logger.py
├── logs/                      # runtime log output (gitignored)
├── data/                      # raw fetched data (gitignored)
├── .env.example               # secrets template
└── pyproject.toml
```

---

## Setup

**Requirements:** Python 3.11+

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # macOS/Linux

# 2. Install dependencies
pip install -e ".[dev]"

# 3. Set up secrets
copy .env.example .env        # Windows
# cp .env.example .env        # macOS/Linux
# Edit .env and fill in your API credentials
```

---

## Running

```bash
python -m src.run
# or, if installed via pyproject.toml scripts:
polymarket-research
```

On startup, `run.py` will:
1. Load and validate `config/settings.yaml`
2. Create `logs/` and `data/` directories
3. Initialize structured logging (JSON to file + readable console)
4. Register SIGINT/SIGTERM shutdown handlers
5. Emit a startup log and begin the heartbeat loop

Stop with `Ctrl+C` or `SIGTERM`.

---

## Configuration

All runtime settings live in `config/settings.yaml`.
API secrets (keys, passphrases) live in `.env` — **never commit `.env`**.

Key settings:

| Section | Key | Default | Description |
|---|---|---|---|
| `project` | `env` | `development` | `development` or `production` |
| `logging` | `level` | `INFO` | Log level |
| `logging` | `console` | `true` | Readable output to stdout |
| `logging` | `json_to_file` | `true` | JSON lines to `logs/` |
| `runner` | `heartbeat_interval_seconds` | `60` | Heartbeat interval |

---

## Testing

```bash
pytest
pytest --cov=src --cov-report=term-missing
```

---

## Linting and Type Checking

```bash
ruff check src tests
ruff format src tests
mypy src
```

---

## Phase Roadmap

| Phase | Scope | Status |
|---|---|---|
| **1** | Foundation: config, logging, heartbeat loop | **In progress** |
| 1 | Polymarket HTTP client + market discovery | Pending |
| 1 | Price/orderbook fetcher + JSONL storage | Pending |
| 2+ | Signal research, feature engineering | Not started |
| 2+ | Strategy logic, backtesting | Not started |
| 3+ | Live execution | Not started |
