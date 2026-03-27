ALTER TABLE orders ADD CONSTRAINT positive_amount CHECK (amount > 0) NOT VALID;
