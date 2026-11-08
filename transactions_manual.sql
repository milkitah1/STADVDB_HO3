-- ADVANCED DATABASE SYSTEMS: CONCURRENCY, ISOLATION & LOCKING EXPERIMENTS
-- Table: film_inventory (InnoDB Engine)

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

-- Helper to reset baseline data between runs:
-- TRUNCATE film_inventory;
-- INSERT INTO film_inventory VALUES (1,101,1,10,'available'),(2,102,1,20,'available'),(3,103,1,30,'available'),(4,104,2,40,'available'),(5,105,2,50,'available');

-- ============================================================================
-- PART 1: 12 BASELINE CASES (3 ANOMALIES x 4 ISOLATION LEVELS)
-- Run Session 1 first, then execute Session 2 while Session 1 is in DO SLEEP.
-- Test across each level:
--   SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
--   SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
--   SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
--   SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;
-- ============================================================================

-- [ANOMALY 1: DIRTY READ TEMPLATE]
-- Session 1 (Writer):
START TRANSACTION;
UPDATE film_inventory SET stock = 5 WHERE inventory_id = 1;
DO SLEEP(5);
ROLLBACK;

-- Session 2 (Reader):
SET SESSION TRANSACTION ISOLATION LEVEL <LEVEL>;
START TRANSACTION;
SELECT stock FROM film_inventory WHERE inventory_id = 1;
COMMIT;


-- [ANOMALY 2: NON-REPEATABLE READ TEMPLATE]
-- Session 1 (Reader):
SET SESSION TRANSACTION ISOLATION LEVEL <LEVEL>;
START TRANSACTION;
SELECT stock FROM film_inventory WHERE inventory_id = 2;
DO SLEEP(5);
SELECT stock FROM film_inventory WHERE inventory_id = 2;
COMMIT;

-- Session 2 (Writer):
START TRANSACTION;
UPDATE film_inventory SET stock = stock + 10 WHERE inventory_id = 2;
COMMIT;


-- [ANOMALY 3: PHANTOM READ TEMPLATE]
-- Session 1 (Reader):
SET SESSION TRANSACTION ISOLATION LEVEL <LEVEL>;
START TRANSACTION;
SELECT COUNT(*) FROM film_inventory WHERE store_id = 1;
DO SLEEP(5);
SELECT COUNT(*) FROM film_inventory WHERE store_id = 1;
UPDATE film_inventory SET status = 'audited' WHERE store_id = 1;
SELECT COUNT(*) FROM film_inventory WHERE status = 'audited';
COMMIT;

-- Session 2 (Writer):
START TRANSACTION;
INSERT INTO film_inventory VALUES (100, 199, 1, 15, 'available');
COMMIT;


-- ============================================================================
-- PART 2: 4 LOCK EXPERIMENTS (1 PER ISOLATION LEVEL)
-- ============================================================================

-- LOCK 1 [READ UNCOMMITTED]: Explicit Table Lock prevents Dirty Read
-- Session 1 (Writer):
LOCK TABLES film_inventory WRITE;
UPDATE film_inventory SET stock = 5 WHERE inventory_id = 1;
DO SLEEP(5);
UNLOCK TABLES;

-- Session 2 (Reader):
SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT stock FROM film_inventory WHERE inventory_id = 1;


-- LOCK 2 [READ COMMITTED]: Shared Row Lock (FOR SHARE) prevents Non-Repeatable Read
-- Session 1 (Reader):
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
START TRANSACTION;
SELECT stock FROM film_inventory WHERE inventory_id = 2 FOR SHARE;
DO SLEEP(5);
SELECT stock FROM film_inventory WHERE inventory_id = 2;
COMMIT;

-- Session 2 (Writer):
UPDATE film_inventory SET stock = stock + 10 WHERE inventory_id = 2;


-- LOCK 3 [REPEATABLE READ]: Exclusive Range Lock (FOR UPDATE) blocks Phantom Insert
-- Session 1 (Reader):
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
START TRANSACTION;
SELECT * FROM film_inventory WHERE store_id = 1 FOR UPDATE;
DO SLEEP(5);
COMMIT;

-- Session 2 (Writer):
INSERT INTO film_inventory VALUES (102, 199, 1, 15, 'available');


-- LOCK 4 [SERIALIZABLE]: Automatic Shared Locks & Deadlock/Timeout Trade-off
-- Session 1 (Reader):
SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;
START TRANSACTION;
SELECT * FROM film_inventory WHERE store_id = 1;
DO SLEEP(5);
COMMIT;

-- Session 2 (Writer):
INSERT INTO film_inventory VALUES (103, 199, 1, 15, 'available');