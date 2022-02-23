# importing libraries

from pymongo import MongoClient

import pandas as pd

client = MongoClient(
    'mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false')

def scoreinput(path):
    df = pd.read_csv(path)
    df=df.to_dict('records')
    client.saraswat.da_leadscore.insert_many(df)

path = "./notebook/locationscore.csv"
scoreinput(path)
path = "./notebook/maincatscore.csv"
scoreinput(path)
path = "./notebook/prodcatscore.csv"
scoreinput(path)
path = "./notebook/productscore.csv"
scoreinput(path)
path = "./notebook/professionscore.csv"
scoreinput(path)
path = "./notebook/sourcescore.csv"
scoreinput(path)
path= "./notebook/statusscore.csv"
scoreinput(path)