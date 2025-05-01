# Kafka-Based Credit Card Transaction Processing System

## Project Overview
This project implements a credit card transaction processing system using a **Lambda Architecture**, featuring:

- **Stream Layer:** Real-time validation and classification of transactions.
- **Batch Layer:** Periodic approval and credit score recalculations.
- **Serving Layer:** MySQL integration for data persistence and querying.


## Dataset Description

- `customers.csv`: Customer details like address, score, and income
- `cards.csv`: Credit card details linked to customers
- `credit_card_types.csv`: Metadata on types of cards
- `transactions.csv`: Raw transaction data (April 1–4, 2025)

## Architecture & Components
```
Producer (MySQL → Kafka)
     ↓
Consumer (Kafka → Validated CSV)
     ↓
Batch Layer (Finalize Transactions, Update Balances/Scores)
     ↓
Push Updates to MySQL
```

### Core Python Scripts
- `main.py`: Orchestrates the entire pipeline in order
- `load_data.py`: Loads initial CSVs into MySQL
- `producer.py`: Streams transactions from MySQL to Kafka
- `consumer.py`: Validates transactions from Kafka and outputs `stream_transactions.csv`
- `batch_processing.py`: Approves pending transactions, updates credit scores/limits
- `push_updates.py`: Pushes updates (cards, customers, batch results) to MySQL
- `helper.py`: Credit score & limit adjustment rules
- `utils.py`: Kafka & MySQL configuration management

## Folder Structure
```
project-root/
├── data/                   # Original CSV data
├── results/                # Output CSVs from stream and batch layers
├── src/                    # Python scripts for stream, batch, utility
├── .env / .env.example     # Environment variables
├── credit_system_schema.sql
├── requirements.txt        # Python dependencies
├── README.md
└── main.py                 # Entry point to run the full pipeline
```

## Environment Variables Setup
Create a `.env` file:
```env
# MySQL Connection Info
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DB=credit_system
MYSQL_USER=your_mysql_user
MYSQL_PASSWORD=your_mysql_password

#  Kafka Configuration
KAFKA_BROKER=localhost:9092

```

## Requirements
- Python 3.8+
- Kafka
- MySQL Server
- Java 8+ (required for Kafka)
- pip (Python package manager)

### Python Packages Required:
- kafka-python
- mysql-connector-python
- python-dotenv

## How to Run

### 1. Download and Setup Kafka

```bash
# Create a directory for Kafka
mkdir -p ~/kafka
cd ~/kafka

# Download Kafka 3.8.1
wget https://downloads.apache.org/kafka/3.8.1/kafka_2.13-3.8.1.tgz

# Extract the archive
tar -xzf kafka_2.13-3.8.1.tgz

# [Optional] Set up environment variables (add these to your ~/.bashrc or ~/.zshrc for permanence)
export KAFKA_HOME=~/kafka/kafka_2.13-3.8.1
export PATH=$PATH:$KAFKA_HOME/bin
```

### 2. Start Kafka Server

```bash
# Start Zookeeper (in a separate terminal)
cd ~/kafka/kafka_2.13-3.8.1
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka server (in another terminal)
cd ~/kafka/kafka_2.13-3.8.1
bin/kafka-server-start.sh config/server.properties
```

### 3. Create Kafka Topic

```bash
# Create the 'transactions' topic
cd ~/kafka/kafka_2.13-3.8.1
bin/kafka-topics.sh --create --topic transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 4. Python Environment Setup

```bash
# Create and activate a virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate

# Install the required packages
pip install -r requirements.txt

# Run the complete pipeline
python main.py
```

This will:
1. Rebuild the MySQL schema (`credit_system_schema.sql`)
2. Load data from CSVs (via `load_data.py`)
3. Start Kafka consumer and producer
4. Wait for you to press Enter after Kafka processing completes
5. Run batch processing
6. Push updates to MySQL

## Output Files
Located in `results/` folder:
- `stream_transactions.csv`: From Kafka consumer
- `batch_transactions.csv`: Finalized approved transactions
- `cards_updated.csv`: New balances and credit limits
- `customers_updated.csv`: Updated credit scores and incomes


## Notes
- Processing is done in sequence: transactions are streamed and handled chronologically
- Declined transactions are printed in real-time during streaming

---