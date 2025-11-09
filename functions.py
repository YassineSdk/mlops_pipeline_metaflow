import pandas as pd 
import sqlite3
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata
import pickle
from pathlib import  Path
import warnings
warnings.filterwarnings('ignore')

#### data generation ####
def synthic_data(nb_rows) -> pd.DataFrame:
    try : 
        path = Path("..\model\synth_model.pkl")
        with open(path,'rb') as f:
            model = pickle.load(f)
    except:
        print('error in model loading')
    try :
        data = model.sample(nb_rows)
    except:
        print('error in data sampling')
    return data

#### database connection #### 

def connect(batch_data):
    try:
        conn = sqlite3("housing_pipeline.db")
    except:
        print('error in the connection')

    try:
        batch_data.to_sql("listings",conn,if_exists="replace",index=False)
    except:
        print('error in the data ingestion')
    print('data is stored successefully')




    



