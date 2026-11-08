-- ADVANCED DATABASE SYSTEMS: CONCURRENCY & ISOLATION EXPERIMENTS
-- Table: film_inventory (Simplified Sakila inventory, InnoDB Engine)

-- 0. SCHEMA SETUP & BASELINE DATA
CREATE DATABASE IF NOT EXISTS sakila_sim;
USE sakila_sim;

DROP TABLE IF EXISTS film_inventory;
CREATE TABLE film_inventory (
    inventory_id INT PRIMARY KEY,
    film_id      INT NOT NULL,
    store_id     INT NOT NULL,
    stock        INT NOT NULL,
    status       VARCHAR(20) NOT NULL DEFAULT 'available',
    INDEX idx_store (store_id)
) ENGINE=InnoDB;

INSERT INTO film_inventory (inventory_id, film_id, store_id, stock, status) VALUES
(1, 101, 1, 10, 'available'),
(2, 102, 1, 20, 'available'),
(3, 103, 1, 30, 'available'),
(4, 104, 2, 40, 'available'),
(5, 105, 2, 50, 'available');

-- 1. EXPERIMENT: DIRTY READ (READ UNCOMMITTED)

-- [SESSION 1 - Writer]
USE sakila_sim;
START TRANSACTION;
UPDATE film_inventory SET stock = 5 WHERE inventory_id = 1;
DO SLEEP(5); -- Holds uncommitted modification
ROLLBACK;        -- Changes are discarded

-- [SESSION 2 - Reader (Run during Session 1's SLEEP)]
USE sakila_sim;
SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
START TRANSACTION;
SELECT inventory_id, stock FROM film_inventory WHERE inventory_id = 1;
COMMIT;


-- 2. EXPERIMENT: NON-REPEATABLE READ (READ COMMITTED)

-- [SESSION 1 - Reader]
USE sakila_sim;
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
START TRANSACTION;
SELECT inventory_id, stock FROM film_inventory WHERE inventory_id = 2; -- First Read: stock = 20
DO SLEEP(5); -- Pause while Session 2 modifies data
SELECT inventory_id, stock FROM film_inventory WHERE inventory_id = 2; -- Second Read: stock = 30 (Changed!)
COMMIT;

-- [SESSION 2 - Writer (Run during Session 1's SLEEP)]
USE sakila_sim;
START TRANSACTION;
UPDATE film_inventory SET stock = 30 WHERE inventory_id = 2;
COMMIT;


-- 3. EXPERIMENT: PHANTOM READ (READ COMMITTED)

-- [SESSION 1 - Reader]
USE sakila_sim;
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
START TRANSACTION;
SELECT COUNT(*) AS initial_count FROM film_inventory WHERE store_id = 1; -- Initial count: 3
DO SLEEP(5); -- Pause while Session 2 inserts new tuple
SELECT COUNT(*) AS phantom_count FROM film_inventory WHERE store_id = 1; -- New count: 4 (Phantom appeared!)
COMMIT;

-- [SESSION 2 - Writer (Run during Session 1's SLEEP)]
USE sakila_sim;
START TRANSACTION;
INSERT INTO film_inventory (inventory_id, film_id, store_id, stock, status) 
VALUES (100, 199, 1, 15, 'available');
COMMIT;


-- 4. EXPERIMENT: LOCKING RESOLUTIONS

-- Prevention of Non-Repeatable Read via Shared Row Lock (FOR SHARE)
-- [Session 1]
USE sakila_sim;
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
START TRANSACTION;
SELECT stock FROM film_inventory WHERE inventory_id = 2 FOR SHARE; -- Acquires Shared (S) Lock
DO SLEEP(5);
SELECT stock FROM film_inventory WHERE inventory_id = 2;
COMMIT;

-- [Session 2 (Run concurrently)]
-- The following UPDATE will be BLOCKED until Session 1 completes, preserving consistency:
USE sakila_sim;
UPDATE film_inventory SET stock = 40 WHERE inventory_id = 2;