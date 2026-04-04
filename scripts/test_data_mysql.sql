DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    age INT,
    dept VARCHAR(100)
);

INSERT INTO users VALUES
    (1, 'Alice', 30, 'Engineering'),
    (2, 'Bob', 25, 'Sales'),
    (3, 'Carol', 35, 'Engineering'),
    (4, 'Dave', 28, 'Sales'),
    (5, 'Eve', 32, 'Engineering');

CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT,
    total DECIMAL(10,2),
    status VARCHAR(50)
);

INSERT INTO orders VALUES
    (101, 1, 150.00, 'completed'),
    (102, 2, 75.50, 'pending'),
    (103, 1, 200.00, 'completed'),
    (104, 3, 50.00, 'cancelled');
