import duckdb
import os


def clean_tables(db_path=None):
    if db_path is None:
        db_path = os.path.join('/opt/airflow/dags', 'tmp', 'data', 'duckdb.db')

    con = duckdb.connect(db_path)

    tables = con.execute("SHOW TABLES").fetchall()
    tables = [t[0] for t in tables]

    for table in tables:
        con.execute(f"""
            CREATE OR REPLACE TABLE {table}_dedup AS
            SELECT DISTINCT * FROM {table}
        """)
        con.execute(f"DROP TABLE {table}")
        con.execute(f"ALTER TABLE {table}_dedup RENAME TO {table}")

    for table in tables:
        # Obter nomes e tipos das colunas
        columns_info = con.execute(f"PRAGMA table_info('{table}')").fetchall()
        # Identificar colunas do tipo epoch (assumindo nome contendo 'epoch')
        epoch_cols = [col[1] for col in columns_info if 'epoch' in col[1].lower()]
        
        if epoch_cols:
            # Construir a lista de colunas, convertendo as epoch para timestamp
            select_cols = []
            for col in [c[1] for c in columns_info]:
                if col in epoch_cols:
                    select_cols.append(f"CAST({col} AS TIMESTAMP) AS {col}_dt")
                    select_cols.append(col)  # mantém a coluna original também
                else:
                    select_cols.append(col)
            select_expr = ", ".join(select_cols)
        else:
            select_expr = "*"
        
        con.execute(f"""
            CREATE OR REPLACE TABLE {table}_dedup AS
            SELECT DISTINCT {select_expr} FROM {table}
        """)
        con.execute(f"DROP TABLE {table}")
        con.execute(f"ALTER TABLE {table}_dedup RENAME TO {table}")

    con.close()
    print("Duplicatas removidas de todas as tabelas. e colunas mudadas pra timestamp")

