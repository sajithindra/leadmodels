from fastapi import FastAPI
from pydantic import BaseModel, Field
from pydantic import validator
from typing import List
import datetime
import pandas as pd
import numpy as np
from pymongo import MongoClient

import numpy as np

from typing import Optional
from bson import ObjectId

# Fetching da_lead data from Mongodb

client = MongoClient(
    'mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false')


class PyObjectId(ObjectId):

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError('Invalid objectid')
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type='string')


class Target(BaseModel):
    og_id: Optional[PyObjectId] = Field(alias='_id')
    zone_id: str
    year: int
    quarters: str
    CASA: str
    CREDIT_CARD: str
    TPP: str
    ADVANCE: str
    LTSD: str
    organization_id: str
    month: str
    monthly_yearly: str = Field(alias='type')
    userwise: int
    approve: int
    created: str
    status: int
    user_id: str
    updated_at: datetime.datetime

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            ObjectId: str
        }

    @validator('zone_id')
    def check_zone(cls, value):
        if value not in ['5d5b94fe68548822621b5be2', '5d5b950768548822a3106892',
                         '5d5b951268548823cc0429b2', '5d5b9522685488250b071352',
                         '5d5b94836854881bf4608ad2', '5d5b94f7685488222445bf42',
                         '5d5b951a68548824b33e38d2', '5d5b957268548828a775b022',
                         '5d5b9529685488256f4bf172', '5d5b95636854882803021d42',
                         '5d5b953168548825ad04f212', '5d5b956a68548828515fdc82',
                         '5d5b9582685488292d76d0b2']:
            raise ValueError('zone_id is not valid')
        return value

    @validator('monthly_yearly')
    def check_type(cls, value):
        if value not in ['monthly', 'yearly']:
            raise ValueError('type value not valid')
        return value

    @validator('userwise')
    def check_userwise(cls, value):
        if value not in [0, 1]:
            raise ValueError('userwise value not valid')
        return value

    @validator('approve')
    def check_approve(cls, value):
        if value not in [0, 1]:
            raise ValueError('approve value not valid')
        return value

    @validator('status')
    def check_status(cls, value):
        if value not in [0, 1]:
            raise ValueError('status not valid')
        return value

    @validator('month')
    def check_month(cls, value):
        if value not in ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August''September', 'October', 'November', 'December']:
            raise ValueError('month not valid')
        return value

    @validator('status')
    def check_status(cls, value):
        if value not in [0, 1]:
            raise ValueError('status not valid')
        return value

    @validator('CASA', 'CREDIT_CARD', 'TPP', 'LTSD', 'ADVANCE')
    def check_product(cls, value):
        assert value.isnumeric(), 'must be numeric'
        return value

    @validator('year')
    def check_year(cls, value):

        if value.isnumeric() == False or len(value) != 4:
            raise ValueError('year not valid')
        return value

    @validator('user_id')
    def check_userid(cls, value):
        assert value.isalnum(), 'must be alphanumeric'
        return value

    @validator('organization_id')
    def check_organization_id(cls, value):
        assert value.isalnum(), 'must be alphanumeric'
        return value


app = FastAPI()
@app.get('/')
async def test():
    return {'404': 'Test was sucessful'}


@app.post('/target_cleaning')
async def predict_stage(d: List[Target]):
    da = []
    for i in d:
        da.append(dict(i))
    df = pd.DataFrame(data=da)

    df.drop(['og_id', 'created', 'updated_at',
             'organization_id'], axis=1, inplace=True)
    df.rename(columns={'monthly_yearly': 'type'}, inplace=True)

    # function to convert empty strings to nan

    def empty_nan(df, col):
        df.loc[(df[col] == ''), col] = np.nan
        return df

    cat_columns = df.select_dtypes(['O']).columns
    for el in cat_columns:
        df = empty_nan(df, el)

    # Adding a user_name column

    # fetching  user data
    result = client['saraswat']['users'].aggregate(
        [{
            "$addFields":
            {
                "user_id": {"$toString": "$_id"}
            }},
            {
            "$project":
            {
                "_id": 0,
                "user_id": 1,
                "name": 1

            }}
         ])

    # converting to dataframe
    user = pd.DataFrame(list(result))
    user.rename(columns={'name': 'user_name'}, inplace=True)

    df = df.merge(user, on="user_id", how="outer")

    # Adding a zone_name column

    # fetching  zone data
    result = client['saraswat']['zone'].aggregate(
        [{
            "$addFields":
            {
                "zone_id": {"$toString": "$_id"}
            }},
            {
            "$project":
            {
                "_id": 0,
                "zone_id": 1,
                "name": 1
            }}
         ])

    # converting to dataframe
    zone = pd.DataFrame(list(result))

    zone.rename(columns={'name': 'zone_name'}, inplace=True)

    df = df.merge(zone, on="zone_id", how="outer")

    # converting month to corresponding numbers
    df['month'] = pd.to_datetime(df.month, format='%B').dt.month

    def target(df, name):
        name_target = df[['zone_id', 'zone_name', 'type', 'year', 'month',
                          'userwise', 'status', 'approve', 'user_id', 'user_name', name]]
        name_target['product_main_category_name'] = name
        name_target.rename(columns={name: 'target'}, inplace=True)
        name_target.dropna(subset=['target'], inplace=True)
        # Adding collecdtion to Mongodb
        name_target.reset_index(drop=True, inplace=True)
        df_dict = name_target.to_dict('records')
        client.saraswat.da_target.insert_many(df_dict)

    target(df, 'CASA')
    target(df, 'ADVANCE')
    target(df, 'CREDIT_CARD')
    target(df, 'TPP')

    ltsd_target = df[['zone_id', 'zone_name', 'type', 'year', 'month',
                      'userwise', 'status', 'approve', 'user_id', 'user_name', 'LTSD']]
    ltsd_target['product_main_category_name'] = 'LTSD/TD'
    ltsd_target.rename(columns={'LTSD': 'target'}, inplace=True)
    ltsd_target.dropna(subset=['target'], inplace=True)
    ltsd_target.reset_index(drop=True, inplace=True)
    df_dict = ltsd_target.to_dict('records')
    client.saraswat.da_target.insert_many(df_dict)
