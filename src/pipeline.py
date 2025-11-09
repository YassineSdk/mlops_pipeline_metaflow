
from prefect import flow,task 
import pandas as pd 
import sqlite3
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata
import pickle
from pathlib import  Path
import warnings
warnings.filterwarnings('ignore')

#### data generation ####
@task
def synthic_data(nb_rows) -> pd.DataFrame:
    path = Path("..\model\synth_model.pkl")
    if not path.exists():
        raise FileNotFoundError(f"Model file not found at {path}")

    with open(path,'rb') as f:
        model = pickle.load(f)

    batch_data = model.sample(nb_rows)

    return batch_data

#### database connection #### 

@task(retries=3,retry_delay_seconds=5)
def connect(batch_data):
    if batch_data is None or batch_data.empty:
        raise ValueError("No data to store in the database")

    db_path = ("housing_pipeline.db")
    with sqlite3.connect(db_path,timeout=30,check_same_thread=False) as conn:
        batch_data.to_sql("listings",conn,if_exists="append",index=False)

    


@flow
def data_pipeline():
    try :
        batch_data = synthic_data(2)
        connect(batch_data)
        print(' the pipeline ran successfully')
    except Exception as e :
        print(f"pipeline failed: {e}")

data_pipeline()
