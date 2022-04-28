#Needed packages
import pandas as pd
from minio import Minio
from sqlalchemy import create_engine

#Establishing connection with minio
client =  Minio('minio:9000', access_key="admin", secret_key="Zaak@1101", secure=False)

#Getting desired file
dataMinio = client.get_object(
    "data-bucket",
    "data/newFollowers.csv",
)

#Converting minio object to pandas dataframe
dataset = pd.read_csv(dataMinio, index_col='Unnamed: 0')

#Converting pandas object to datetime
dataset['Date'] = pd.to_datetime(dataset['Date'])

#Lets see total days collected
print(f'Total Days: {len(dataset)} Day')

#Replacing columns name with small letters
dataset.columns = ['date','sponsored_followers','organic_followers','total_followers']

#Connecting to our dockerized postgresql db
engine = create_engine('postgresql://zaak:Zaak1234@postgres:5432/datadb')

#pushing dataset to the database
dataset.to_sql('newFollowers', engine)