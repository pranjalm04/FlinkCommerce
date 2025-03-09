CREATE DATABASE flink_db;

\c flink_db;

CREATE TABLE IF NOT EXISTS currency_conversion_rates(

    from_currency VARCHAR(3),   -- Currency code (e.g., USD)
    to_currency VARCHAR(3),     -- Currency code (e.g., EUR)
    exchange_rate DECIMAL(18, 6) NOT NULL CHECK (exchange_rate > 0),
    last_updated TIMESTAMP NOT NULL,
    PRIMARY KEY (from_currency, to_currency)
);

-- Drop existing function and trigger if they exist
DROP TRIGGER IF EXISTS check_last_updated ON currency_conversion_rates;
DROP FUNCTION IF EXISTS enforce_last_updated_check;

-- Create function to enforce last_updated constraint
CREATE OR REPLACE FUNCTION enforce_last_updated_check()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.last_updated > NOW() THEN
        RAISE EXCEPTION 'last_updated must be less than or equal to the current timestamp';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to call the function before insert or update
CREATE TRIGGER check_last_updated
BEFORE INSERT OR UPDATE ON currency_conversion_rates
FOR EACH ROW
EXECUTE FUNCTION enforce_last_updated_check();


CREATE TABLE IF NOT EXISTS transactions (
transaction_id VARCHAR(255) PRIMARY KEY,
product_id VARCHAR(255),
product_name VARCHAR(255),
product_category VARCHAR(255),
product_price DOUBLE PRECISION,
product_quantity INTEGER,
product_brand VARCHAR(255),
total_amount DOUBLE PRECISION,
currency VARCHAR(255),
customer_id VARCHAR(255),
transaction_date TIMESTAMP,
payment_method VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS sales_per_category (
transaction_date DATE,
category VARCHAR(255),
total_sales DOUBLE PRECISION,
PRIMARY KEY (transaction_date, category)
);

CREATE TABLE IF NOT EXISTS sales_per_day (
transaction_date DATE PRIMARY KEY,
total_sales DOUBLE PRECISION
);
