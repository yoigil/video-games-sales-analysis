from sqlalchemy import create_engine
import pandas as pd
data = pd.read_csv('P2M3_adriel_julius_sutanto_data_raw.csv')
engine = create_engine('postgresql://airflow:airflow@localhost:5434/airflow')
data.to_sql('table_m3', engine)