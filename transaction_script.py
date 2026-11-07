"""
STADVDB - Concurrency & Isolation Levels: automated experiment runner.

Simulates two concurrent transactions (two threads, two connections) for:
  * Dirty read, Non-repeatable read, Phantom read
  * across READ UNCOMMITTED / READ COMMITTED / REPEATABLE READ / SERIALIZABLE
and then tests locking fixes (row locks, range locks, table locks) plus a
small throughput benchmark.

Setup:
    pip install mysql-connector-python
    export DB_HOST=127.0.0.1 DB_USER=root DB_PASS=yourpassword
    python concurrency_runner.py

Results are printed and saved to results.csv
"""
import csv
import os
import threading
import time

import mysql.connector

CFG = dict(
    host=os.getenv("DB_HOST", "127.0.0.1"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASS", "123456"),
    autocommit=True,
)
DB = "sakila_tx"
LEVELS = ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]
HOLD = 3        # seconds the "long" transaction holds its position (DO SLEEP equivalent)
LAG = 1         # seconds the second transaction waits before starting


# ----------------------------------------------------------------- helpers
def conn(level=None):
    c = mysql.connector.connect(database=DB, **CFG)
    cur = c.cursor()
    cur.execute("SET SESSION innodb_lock_wait_timeout = 15")
    if level:
        cur.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {level}")
    cur.close()
    return c


def q(c, sql, fetch=None, params=None):
    cur = c.cursor(buffered=True)

    cur.execute(sql, params or ())

    if fetch == "one":
        result = cur.fetchone()
    elif fetch == "all":
        result = cur.fetchall()
    elif cur.with_rows:
        result = cur.fetchall()
    else:
        result = None

    cur.close()
    return result


def setup():
    c = mysql.connector.connect(**CFG)
    cur = c.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB}")
    cur.execute(f"USE {DB}")
    cur.execute("DROP TABLE IF EXISTS film_inventory")
    cur.execute("""
        CREATE TABLE film_inventory (
            inventory_id INT NOT NULL PRIMARY KEY,
            film_id      INT NOT NULL,
            store_id     INT NOT NULL,
            stock        INT NOT NULL,
            status       VARCHAR(20) NOT NULL DEFAULT 'available',
            INDEX idx_store (store_id)
        ) ENGINE=InnoDB""")
    cur.close()
    c.close()


def reset():
    c = conn()
    q(c, "DELETE FROM film_inventory")
    q(c, """INSERT INTO film_inventory VALUES
            (1,101,1,10,'available'),(2,102,1,20,'available'),
            (3,103,1,30,'available'),(4,104,2,40,'available'),
            (5,105,2,50,'available')""")
    c.close()


def run_pair(t1, t2):
    """Run two transaction functions concurrently; capture exceptions."""
    errors = {}

    def wrap(name, fn):
        def inner():
            try:
                fn()
            except Exception as e:  # lock timeout, deadlock, etc.
                errors[name] = str(e)
        return threading.Thread(target=inner)

    a, b = wrap("T1", t1), wrap("T2", t2)
    a.start(); b.start()
    a.join(); b.join()
    return errors


# ------------------------------------------------------------- anomalies
def dirty_read(level):
    reset()
    r = {}

    def writer():                       # T1: update, hold, ROLLBACK
        c = conn()
        q(c, "START TRANSACTION")
        q(c, "UPDATE film_inventory SET stock = stock - 5 WHERE inventory_id = 1")
        time.sleep(HOLD)
        q(c, "ROLLBACK")
        c.close()

    def reader():                       # T2: read while T1 is uncommitted
        time.sleep(LAG)
        c = conn(level)
        q(c, "START TRANSACTION")
        t = time.time()
        r["seen"] = q(c, "SELECT stock FROM film_inventory WHERE inventory_id = 1", "one")
        r["wait"] = time.time() - t
        q(c, "COMMIT")
        c.close()

    err = run_pair(writer, reader)
    seen = r.get("seen")
    anomaly = seen == 5                 # original = 10, uncommitted = 5
    return anomaly, f"original=10 uncommitted=5 reader_saw={seen} reader_wait={r.get('wait', 0):.1f}s", err


def non_repeatable_read(level, reader_sql_suffix="", writer_level="READ COMMITTED"):
    reset()
    r = {}

    def reader():                       # T1: read, hold, read again
        c = conn(level)
        q(c, "START TRANSACTION")
        r["r1"] = q(c, f"SELECT stock FROM film_inventory WHERE inventory_id = 2 {reader_sql_suffix}", "one")
        time.sleep(HOLD)
        r["r2"] = q(c, f"SELECT stock FROM film_inventory WHERE inventory_id = 2 {reader_sql_suffix}", "one")
        q(c, "COMMIT")
        c.close()

    def writer():                       # T2: update + commit in between
        time.sleep(LAG)
        c = conn(writer_level)
        t = time.time()
        q(c, "START TRANSACTION")
        q(c, "UPDATE film_inventory SET stock = stock + 10 WHERE inventory_id = 2")
        q(c, "COMMIT")
        r["writer_wait"] = time.time() - t
        c.close()

    err = run_pair(reader, writer)
    anomaly = r.get("r1") != r.get("r2")
    return anomaly, f"read1={r.get('r1')} read2={r.get('r2')} writer_wait={r.get('writer_wait', 0):.1f}s", err


def phantom_read(level, locking=False, writer_level="READ COMMITTED"):
    reset()
    r = {}
    suffix = "FOR UPDATE" if locking else ""

    def reader():
        c = conn(level)
        q(c, "START TRANSACTION")
        r["c1"] = len(q(c, f"SELECT inventory_id FROM film_inventory WHERE store_id = 1 {suffix}", "all"))
        time.sleep(HOLD)
        r["c2"] = len(q(c, f"SELECT inventory_id FROM film_inventory WHERE store_id = 1 {suffix}", "all"))
        # MySQL-specific probe: a WRITE sees latest committed data, exposing
        # phantoms hidden by the REPEATABLE READ snapshot.
        q(c, "UPDATE film_inventory SET status = 'audited' WHERE store_id = 1")
        r["audited"] = q(c, "SELECT COUNT(*) FROM film_inventory WHERE status = 'audited'", "one")
        q(c, "COMMIT")
        c.close()

    def writer():
        time.sleep(LAG)
        c = conn(writer_level)
        t = time.time()
        q(c, "START TRANSACTION")
        q(c, "INSERT INTO film_inventory VALUES (100,199,1,15,'available')")
        q(c, "COMMIT")
        r["writer_wait"] = time.time() - t
        c.close()

    err = run_pair(reader, writer)
    anomaly = r.get("c1") != r.get("c2") or r.get("audited") != r.get("c1")
    return anomaly, (f"count1={r.get('c1')} count2={r.get('c2')} audited_rows={r.get('audited')} "
                     f"writer_wait={r.get('writer_wait', 0):.1f}s"), err


# ----------------------------------------------------------- lock fixes
def table_lock_read(writer_level="READ COMMITTED"):
    """Explicit LOCK TABLES ... READ held by the reader; writer touches an UNRELATED row."""
    reset()
    r = {}

    def reader():
        c = conn()
        q(c, "LOCK TABLES film_inventory READ")
        r["c1"] = q(c, "SELECT COUNT(*) FROM film_inventory WHERE store_id = 1", "one")
        time.sleep(HOLD)
        r["c2"] = q(c, "SELECT COUNT(*) FROM film_inventory WHERE store_id = 1", "one")
        q(c, "UNLOCK TABLES")
        c.close()

    def writer():
        time.sleep(LAG)
        c = conn(writer_level)
        t = time.time()
        q(c, "UPDATE film_inventory SET stock = stock + 1 WHERE inventory_id = 5")  # store 2 row
        r["writer_wait"] = time.time() - t
        c.close()

    err = run_pair(reader, writer)
    return r.get("c1") != r.get("c2"), \
        f"count1={r.get('c1')} count2={r.get('c2')} unrelated_row_writer_wait={r.get('writer_wait', 0):.1f}s", err


def row_lock_unrelated_write():
    """Row lock on row 2 does NOT block a writer on a different row (concurrency preserved)."""
    reset()
    r = {}

    def reader():
        c = conn("READ COMMITTED")
        q(c, "START TRANSACTION")
        q(c, "SELECT stock FROM film_inventory WHERE inventory_id = 2 FOR SHARE")
        time.sleep(HOLD)
        q(c, "COMMIT")
        c.close()

    def writer():
        time.sleep(LAG)
        c = conn("READ COMMITTED")
        t = time.time()
        q(c, "UPDATE film_inventory SET stock = stock + 1 WHERE inventory_id = 5")
        r["writer_wait"] = time.time() - t
        c.close()

    err = run_pair(reader, writer)
    return False, f"unrelated_row_writer_wait={r.get('writer_wait', 0):.2f}s", err


# ------------------------------------------------------------ benchmark
def benchmark(mode, workers=8, txns=15):
    """
    Throughput under two strategies:
      row   : START TRANSACTION; SELECT ... FOR UPDATE (own row); UPDATE; COMMIT
      table : LOCK TABLES film_inventory WRITE; UPDATE; UNLOCK TABLES
    Each txn does ~20ms of simulated work while holding its lock.
    """
    reset()

    def worker(i):
        c = conn("REPEATABLE READ")
        row = (i % 5) + 1
        for _ in range(txns):
            if mode == "row":
                q(c, "START TRANSACTION")
                q(c, f"SELECT stock FROM film_inventory WHERE inventory_id = {row} FOR UPDATE")
                time.sleep(0.02)
                q(c, f"UPDATE film_inventory SET stock = stock + 1 WHERE inventory_id = {row}")
                q(c, "COMMIT")
            else:
                q(c, "LOCK TABLES film_inventory WRITE")
                time.sleep(0.02)
                q(c, f"UPDATE film_inventory SET stock = stock + 1 WHERE inventory_id = {row}")
                q(c, "UNLOCK TABLES")
        c.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(workers)]
    t0 = time.time()
    [t.start() for t in threads]
    [t.join() for t in threads]
    dur = time.time() - t0
    return dur, workers * txns / dur


# ----------------------------------------------------------------- main
def main():
    setup()
    rows = []

    def record(exp, level, anomaly, detail, err):
        verdict = "ANOMALY" if anomaly else "no anomaly"
        if err:
            verdict += f" (errors: {err})"
        rows.append([exp, level, verdict, detail])
        print(f"{exp:<34} {level:<17} {verdict:<12} {detail}")

    print("\n=== PART 1: Read anomalies x isolation levels ===")
    for lvl in LEVELS:
        record("Dirty read", lvl, *dirty_read(lvl))
    for lvl in LEVELS:
        record("Non-repeatable read", lvl, *non_repeatable_read(lvl))
    for lvl in LEVELS:
        record("Phantom read", lvl, *phantom_read(lvl))

    print("\n=== PART 2: Locking fixes ===")
    record("NRR + SELECT..FOR SHARE", "READ COMMITTED",
           *non_repeatable_read("READ COMMITTED", "FOR SHARE"))
    record("NRR + SELECT..FOR UPDATE", "READ COMMITTED",
           *non_repeatable_read("READ COMMITTED", "FOR UPDATE"))
    record("Phantom + range FOR UPDATE", "READ COMMITTED",
           *phantom_read("READ COMMITTED", locking=True))
    record("Phantom + range FOR UPDATE", "REPEATABLE READ",
           *phantom_read("REPEATABLE READ", locking=True))
    record("LOCK TABLES READ (table lock)", "n/a", *table_lock_read())
    record("Row lock, unrelated row write", "READ COMMITTED", *row_lock_unrelated_write())

    print("\n=== PART 3: Throughput trade-off (8 workers x 15 txns) ===")
    for mode in ("row", "table"):
        dur, tps = benchmark(mode)
        detail = f"total={dur:.2f}s throughput={tps:.1f} txn/s"
        rows.append(["Benchmark", f"{mode}-level lock", "-", detail])
        print(f"Benchmark {mode + '-level lock':<24} {detail}")

    with open("results.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["experiment", "isolation_level", "verdict", "detail"])
        w.writerows(rows)
    print("\nSaved results.csv")


if __name__ == "__main__":
    main()