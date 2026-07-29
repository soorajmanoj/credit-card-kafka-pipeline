![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?logo=apachekafka&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-4479A1?logo=mysql&logoColor=white)
[![CI](https://github.com/soorajmanoj/credit-card-kafka-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/soorajmanoj/credit-card-kafka-pipeline/actions/workflows/ci.yml)

# Kafka-Based Credit Card Transaction Processing System

A real-time + batch transaction processing system built on a **Lambda Architecture** — transactions are validated and classified the moment they arrive, then reconciled and scored on a batch cycle. This is the same general split real payment processors use: fast provisional decisions at the edge, slower and more thorough reconciliation downstream. The implementation here is a small-scale, single-machine version of that idea, not a claim of production-grade parity — no chargebacks, no ACH/network settlement rules, no multi-node fault tolerance.

## Why Lambda Architecture?

A pure real-time system would have to make every decision (approve, decline, update credit score) instantly, with no room to reconsider. A pure batch system would be accurate but slow, missing the fraud-check use case entirely. Lambda architecture splits the difference: the **stream layer** makes fast, provisional decisions as transactions arrive, and the **batch layer** periodically reconciles and finalizes them — approving pending transactions and recalculating credit scores with the benefit of a wider view of the data.

## Architecture

```
Producer (MySQL → Kafka)
     ↓
Consumer (Kafka → Validated CSV)
     ↓
Batch Layer (Finalize Transactions, Update Balances/Scores)
     ↓
Push Updates to MySQL
```

- **Stream Layer:** Real-time validation and classification of incoming transactions
- **Batch Layer:** Periodic approval and credit score recalculation
- **Serving Layer:** MySQL for persistence and querying

## Example Output

**Same transaction, before and after reconciliation** — this is the Lambda architecture split in practice, not just in theory. The stream layer marks it `pending` the instant it's classified; the batch layer reconciles it to `approved` on the next run.

`results/stream_transactions.csv` (stream layer, provisional):
```
transaction_id,card_id,merchant_name,timestamp,amount,location,transaction_type,related_transaction_id,status,pending_balance
1,7,"Harris, Delacruz and Fuller",2025-04-01T01:00:12,312.76,"1116 Jones Circle, Troy, NY",p2p_transfer,,pending,705.92
```

`results/batch_transactions.csv` (batch layer, finalized):
```
transaction_id,card_id,merchant_name,timestamp,amount,location,transaction_type,related_transaction_id,status,pending_balance
1,7,"Harris, Delacruz and Fuller",2025-04-01T01:00:12,312.76,"1116 Jones Circle, Troy, NY",p2p_transfer,,approved,705.92
```

**Real-time decline, straight from the stream consumer log:**
```
Declined transaction 236 — Amount >= 50% of credit limit
```

## Scale & Results

From a single end-to-end run against the included synthetic dataset:

- **404 transactions** streamed through the pipeline in one pass
- **10 declined at the stream layer (~2.5%)** — 9 for a distance-based fraud check ("merchant too far from customer"), 1 for exceeding 50% of the card's credit limit
- Batch layer reconciled all pending transactions and pushed updates back to MySQL — verified end-to-end via `SELECT COUNT(*) FROM stream_transactions` returning all 404 rows
- Runs fully unattended aside from one confirmation prompt between the stream and batch phases
- Full run (schema rebuild → load → stream → batch → MySQL push): ~1 minute wall time on a local dev machine — note this includes the manual confirmation pause built into `main.py`, so it reflects a full working session more than raw throughput

## Dataset Description

- `customers.csv`: Customer details like address, score, and income
- `cards.csv`: Credit card details linked to customers
- `credit_card_types.csv`: Metadata on types of cards
- `transactions.csv`: Raw transaction data (April 1–4, 2025)

## Core Python Scripts

| Script | Role |
|---|---|
| `main.py` | Orchestrates the entire pipeline in order |
| `load_data.py` | Loads initial CSVs into MySQL |
| `producer.py` | Streams transactions from MySQL to Kafka |
| `consumer.py` | Validates transactions from Kafka, outputs `stream_transactions.csv` |
| `batch_processing.py` | Approves pending transactions, updates credit scores/limits |
| `push_updates.py` | Pushes updates (cards, customers, batch results) to MySQL |
| `helper.py` | Credit score & limit adjustment rules |
| `utils.py` | Kafka & MySQL configuration management |

## Folder Structure

```
project-root/
├── data/                   # Original CSV data
├── results/                # Output CSVs from stream and batch layers
├── src/                    # Python scripts for stream, batch, utility
├── tests/                  # pytest suite (helper, batch_processing, consumer)
├── .github/workflows/      # CI: runs pytest on every push/PR
├── .env / .env.example     # Environment variables
├── credit_system_schema.sql
├── requirements.txt        # Python dependencies
├── requirements-dev.txt    # pytest (not needed to just run the pipeline)
├── pytest.ini
├── README.md
└── main.py                 # Entry point to run the full pipeline
```

## Setup

### Requirements
- Python 3.8+
- Apache Kafka
- MySQL Server
- Java 8+ (required for Kafka)
- pip

### Environment Variables

Create a `.env` file:

```
# MySQL Connection Info
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DB=credit_system
MYSQL_USER=your_mysql_user
MYSQL_PASSWORD=your_mysql_password

# Kafka Configuration
KAFKA_BROKER=localhost:9092
```

### Python Packages

- kafka-python
- mysql-connector-python
- python-dotenv

## How to Run

### 1. Download and Set Up Kafka

```bash
mkdir -p ~/kafka
cd ~/kafka

wget https://downloads.apache.org/kafka/3.8.1/kafka_2.13-3.8.1.tgz
tar -xzf kafka_2.13-3.8.1.tgz

export KAFKA_HOME=~/kafka/kafka_2.13-3.8.1
export PATH=$PATH:$KAFKA_HOME/bin
```

### 2. Start Kafka

```bash
# Zookeeper (separate terminal)
cd ~/kafka/kafka_2.13-3.8.1
bin/zookeeper-server-start.sh config/zookeeper.properties

# Kafka server (another terminal)
cd ~/kafka/kafka_2.13-3.8.1
bin/kafka-server-start.sh config/server.properties
```

### 3. Create the Kafka Topic

```bash
cd ~/kafka/kafka_2.13-3.8.1
bin/kafka-topics.sh --create --topic transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 4. Set Up Python and Run

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

pip install -r requirements.txt
python main.py
```

This will:
1. Rebuild the MySQL schema (`credit_system_schema.sql`)
2. Load data from CSVs (`load_data.py`)
3. Start the Kafka consumer and producer
4. Wait for you to press Enter once Kafka processing completes
5. Run batch processing
6. Push updates to MySQL

## Output Files

Located in `results/`:
- `stream_transactions.csv` — from the Kafka consumer
- `batch_transactions.csv` — finalized approved transactions
- `cards_updated.csv` — new balances and credit limits
- `customers_updated.csv` — updated credit scores and incomes

## Testing

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest -v
```

Tests cover the parts of the pipeline that don't require a live Kafka broker or MySQL server: the credit-score/limit/location rules in `helper.py`, the full batch-layer reconciliation flow in `batch_processing.py` (using in-memory data instead of real CSVs), and the stream-layer decline/approve logic in `consumer.py` (with `KafkaConsumer` and the MySQL connection mocked so only the validation rules themselves are under test). `producer.py`, `load_data.py`, `push_updates.py`, and `main.py` are thin orchestration around real Kafka/MySQL calls and aren't covered — they're exercised by actually running the pipeline end-to-end. Every push and pull request to `main` runs this suite via GitHub Actions (CI badge above).

## Notes

- Processing happens in sequence: transactions are streamed and handled chronologically
- Declined transactions print in real time during streaming

## Background

This project started as an assignment for RIT DSCI 644 (Spring 2025) — the course provided a starting repo template and a `helper.py` stub with three baseline functions (location check, credit score adjustment, credit limit calculation). Everything else — `main.py`, the Kafka producer/consumer, batch reconciliation, MySQL integration, the finished `helper.py` logic, the test suite, and CI — is my own work, built out and revised well beyond the original assignment.

## License

MIT — see [LICENSE](LICENSE).
