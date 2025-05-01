-- Drop and create the database
DROP DATABASE IF EXISTS credit_system;
CREATE DATABASE credit_system;
USE credit_system;

-- 1. cards (with card_number)
CREATE TABLE cards (
    card_id INT PRIMARY KEY,
    customer_id INT,
    card_type_id INT,
    card_number VARCHAR(30),
    expiration_date DATE,
    credit_limit DECIMAL(10,1),
    current_balance DECIMAL(10,2),
    issue_date DATE
);

-- 2. customers
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(50),
    phone_number VARCHAR(20),
    address VARCHAR(100),
    email VARCHAR(50),
    credit_score DECIMAL(5,1),
    annual_income DECIMAL(10,1)
);

-- 3. credit_card_types
CREATE TABLE credit_card_types (
    card_type_id INT PRIMARY KEY,
    name VARCHAR(50),
    credit_score_min INT,
    credit_score_max INT,
    credit_limit_min INT,
    credit_limit_max INT,
    annual_fee INT,
    rewards_rate DECIMAL(5,3)
);

-- 4. transactions
CREATE TABLE transactions (
    transaction_id INT PRIMARY KEY,
    card_id INT,
    merchant_name VARCHAR(100),
    timestamp DATETIME,
    amount DECIMAL(10,2),
    location VARCHAR(100),
    transaction_type VARCHAR(50),
    related_transaction_id INT
);

-- 5. stream_transactions
CREATE TABLE stream_transactions (
    transaction_id INT PRIMARY KEY,
    card_id INT,
    merchant_name VARCHAR(100),
    timestamp DATETIME,
    amount DECIMAL(10,2),
    location VARCHAR(100),
    transaction_type VARCHAR(50),
    related_transaction_id INT,
    status VARCHAR(20),
    pending_balance DECIMAL(10,2)
);

-- 6. batch_transactions
CREATE TABLE batch_transactions LIKE stream_transactions;