import re
import secrets
import jwt
import datetime
from pymongo import MongoClient
# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB+Compass&directConnection=true&ssl=false')
def createtoken(email):
    token =secrets.token_urlsafe(16)
    dt =datetime.datetime.now()

    result = client['saraswat']['users'].update_one(
    {
    'email':email, 
    },
    {'$set':{'da_key': token}}
    )
    return {'100': 'Key created and inserted to user collection successfully'}
def verify_user(email,data):
    key = dict(client['saraswat']['users'].find_one({'email':email},{'_id':0,'da_key':1}))
    data=jwt.decode(data,key['da_key'],algorithm='HS256')
    print(data)

#createtoken("hello@salesfokuz.com")
data = {'email' : 'hello@salesfokuz.com', 'password' : '$2y$12$4qb2JEzC0bzeNXk/yK8HCO.MWo1nkj/cLNUzbdJiLfdgZYsWRZfPi'}
key = dict(client['saraswat']['users'].find_one({'email':'hello@salesfokuz.com'},{'_id':0,'da_key':1}))
data=jwt.encode(data,key,algorithm='HS256')
verify_user('hello@salesfokuz.com',data)