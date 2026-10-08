CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE customers REPLICA IDENTITY FULL;

INSERT INTO customers (first_name, last_name, email) 
VALUES ('Иван', 'Иванов', 'ivan@example.com');
