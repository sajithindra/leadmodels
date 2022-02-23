import re
from pymongo import MongoClient
client = MongoClient(
    'mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false')

def lspull(key, value):
    if client.saraswat.da_leadscore.count_documents({key:value}) ==0:
               return 0
    else : 
            
        filter={
            key: value
            }
        project={
                '_id': 0, 
                'leadscore': 1
            }

        result = client['saraswat']['da_leadscore'].find_one(
            filter=filter,
            projection=project
            )
        return dict(result)['leadscore']