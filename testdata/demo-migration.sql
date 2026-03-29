ALTER TABLE orders ALTER COLUMN total TYPE numeric(12,2);
CREATE INDEX idx_orders_status ON orders (status);
ALTER TABLE orders ADD CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES customers(id);
