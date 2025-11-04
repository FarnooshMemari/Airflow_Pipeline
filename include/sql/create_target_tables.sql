-- Target schema/table for the merged dataset
CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.customer_orders (
    customer_id INT,
    customer_name TEXT,
    signup_date DATE,
    order_id INT,
    order_date DATE,
    item TEXT,
    quantity INT,
    unit_price NUMERIC(10,2),
    order_amount NUMERIC(12,2)
);
