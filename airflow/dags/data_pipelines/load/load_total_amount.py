import duckdb
import os

def load_pix_total_amount():
  db_path = "/opt/airflow/dags/tmp/data/duckdb.db"
  con = duckdb.connect(db_path)
  # Load the SQL query from the file  
  with open("/opt/airflow/dags/data_pipelines/sql/load/pix_total_amount.sql", "r") as sql_file:
    query = sql_file.read()
  df = con.execute(query).df()
  print(f"Data from accounts table:\n{df}")
  print(f"Colunas da tabela accounts: {list(df.columns)}")

  parquet_path = "/opt/airflow/dags/tmp/data/parquet/pix_total_amount.parquet"

  os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

  con.execute(f"""
    COPY ({query}) TO '{parquet_path}' (FORMAT PARQUET)
  """)
  print(f"Tabela parquet salva em: {parquet_path}")
  con.close()
  os.remove(db_path)
  print(f"Arquivo DuckDB removido: {db_path}")
