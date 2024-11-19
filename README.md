# C2 ETL Workflow

A Prefect-based ETL workflow project.

## Installation

```bash
poetry install
```

## Environment Setup

Create a `.env` file with the following content:
```
PREFECT_API_URL=http://127.0.0.1:4200/api
PREFECT_SERVER_API_HOST=127.0.0.1
PREFECT_SERVER_API_PORT=4200
```

## Usage

1. Start the Prefect server:
```bash
poetry run prefect server start
```

2. In another terminal, run the ETL flow:
```bash
poetry run python -m c2.main
```

Or use the installed script:
```bash
poetry run etl
```

3. Monitor your flows at http://127.0.0.1:4200

## Development

To install development dependencies:
```bash
poetry install --with dev
```
