from utils.save_to_duckdb import import_to_duckdb
import os

def import_all_seeds_to_duckdb(seeds_dir='/opt/airflow/dags/seeds/', db_path='/opt/airflow/dags/tmp/data/duckdb.db'):
    # Importa todos os arquivos CSV da pasta seeds para o DuckDB
    for arquivo in os.listdir(seeds_dir):
        file_path = os.path.join(seeds_dir, arquivo)
        if arquivo.endswith('.csv') and os.path.isfile(file_path):
            print(f'Importando {arquivo} para o DuckDB')
            import_to_duckdb(file_path, conn=db_path)



