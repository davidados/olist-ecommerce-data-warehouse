from pathlib import Path
from db_connection import get_connection


DATA_DIR = Path("data/raw")

TABLE_MAPPING = {
    "olist_customers_dataset.csv": "raw.olist_customers",
    "olist_geolocation_dataset.csv": "raw.olist_geolocation",
    "olist_order_items_dataset.csv": "raw.olist_order_items",
    "olist_order_payments_dataset.csv": "raw.olist_order_payments",
    "olist_order_reviews_dataset.csv": "raw.olist_order_reviews",
    "olist_orders_dataset.csv": "raw.olist_orders",
    "olist_products_dataset.csv": "raw.olist_products",
    "olist_sellers_dataset.csv": "raw.olist_sellers",
    "product_category_name_translation.csv": "raw.product_category_translation",
}


def create_etl_run(conn):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO audit.etl_run_log (
                pipeline_name,
                status
            )
            VALUES (
                'load_raw_olist_csv',
                'RUNNING'
            )
            RETURNING run_id;
        """)
        run_id = cur.fetchone()[0]

    conn.commit()
    return run_id


def update_etl_run_success(conn, run_id, rows_read, rows_loaded):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE audit.etl_run_log
            SET
                finished_at = NOW(),
                status = 'SUCCESS',
                rows_read = %s,
                rows_loaded = %s
            WHERE run_id = %s;
        """, (rows_read, rows_loaded, run_id))

    conn.commit()


def update_etl_run_failed(conn, run_id, error_message):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE audit.etl_run_log
            SET
                finished_at = NOW(),
                status = 'FAILED',
                error_message = %s
            WHERE run_id = %s;
        """, (str(error_message), run_id))

    conn.commit()


def log_file_load(conn, run_id, source_file, target_table, rows_loaded):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO audit.file_load_log (
                run_id,
                source_file,
                target_table,
                rows_loaded
            )
            VALUES (%s, %s, %s, %s);
        """, (run_id, source_file, target_table, rows_loaded))

    conn.commit()


def get_table_count(conn, target_table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {target_table};")
        return cur.fetchone()[0]


def truncate_raw_tables(conn):
    with conn.cursor() as cur:
        for target_table in TABLE_MAPPING.values():
            cur.execute(f"TRUNCATE TABLE {target_table};")

    conn.commit()


def load_csv_with_copy(conn, file_name, target_table):
    file_path = DATA_DIR / file_name

    if not file_path.exists():
        raise FileNotFoundError(f"Missing file: {file_path}")

    rows_before = get_table_count(conn, target_table)

    copy_sql = f"""
        COPY {target_table}
        FROM STDIN
        WITH (
            FORMAT CSV,
            HEADER TRUE,
            DELIMITER ',',
            ENCODING 'UTF8'
        );
    """

    with conn.cursor() as cur:
        with open(file_path, "r", encoding="utf-8") as file:
            cur.copy_expert(copy_sql, file)

    conn.commit()

    rows_after = get_table_count(conn, target_table)
    rows_loaded = rows_after - rows_before

    return rows_loaded


def main():
    conn = get_connection()
    run_id = None

    total_rows_read = 0
    total_rows_loaded = 0

    try:
        run_id = create_etl_run(conn)

        # Fejlesztés közben újrafuttathatóvá tesszük a raw betöltést.
        truncate_raw_tables(conn)

        for file_name, target_table in TABLE_MAPPING.items():
            print(f"Loading {file_name} -> {target_table}")

            rows_loaded = load_csv_with_copy(
                conn=conn,
                file_name=file_name,
                target_table=target_table,
            )

            total_rows_read += rows_loaded
            total_rows_loaded += rows_loaded

            log_file_load(
                conn=conn,
                run_id=run_id,
                source_file=file_name,
                target_table=target_table,
                rows_loaded=rows_loaded,
            )

            print(f"Loaded {rows_loaded:,} rows into {target_table}")

        update_etl_run_success(
            conn=conn,
            run_id=run_id,
            rows_read=total_rows_read,
            rows_loaded=total_rows_loaded,
        )

        print("Raw loading completed successfully.")

    except Exception as exc:
        if run_id is not None:
            update_etl_run_failed(conn, run_id, exc)

        print("Raw loading failed.")
        print(exc)
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()