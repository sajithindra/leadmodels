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


class Achieved(BaseModel):
    og_id: Optional[PyObjectId] = Field(alias='_id')
    user_id: str
    lead_id: int
    download_status: int
    organization_id: str
    date: str
    timestamp: str
    feedback: str
    product_id: str
    achieved: float
    year: int
    month: str
    created: str
    status: int
    manager_id: str
    status: int
    updated_at: datetime.datetime
    category_id: str
    approved_date: str
    comment: str
    data_transfer: str
    transfer_note: str
    update_22_02_2021: float
    sale_month_changed: float
    sub_category_id: str
    lock_status: str
    account_number: str
    amount: float
    server_date: str

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            ObjectId: str
        }

    @validator('organization_id', 'lead_id', 'user_id', 'product_id', 'manager_id', 'category_id')
    def check_ids(cls, value):
        assert value.isalnum(), 'must be alphanumeric'
        return value

    @validator('year')
    def check_year(cls, value):

        if value.isnumeric() == False or len(value) != 4:
            raise ValueError('year not valid')
        return value

    @validator('month')
    def check_month(cls, value):
        if value not in ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August''September', 'October', 'November', 'December']:
            raise ValueError('month not valid')
        return value

    @validator('achieved')
    def check_product(cls, value):
        assert value.isnumeric(), 'must be numeric'
        return value

    @validator('download_status')
    def check_download_status(cls, value):
        if value not in [0, 1]:
            raise ValueError('download_status not valid')
        return value

    @validator('status')
    def check_status(cls, value):
        if value not in [0, 1, 2, 3, 4, 5]:
            raise ValueError('status not valid')
        return value

    @validator('CASA', 'CREDIT_CARD', 'TPP', 'LTSD', 'ADVANCE')
    def check_product(cls, value):
        assert value.isnumeric(), 'must be numeric'
        return value


app = FastAPI()
@app.get('/')
async def test():
    return {'404': 'Test was sucessful'}


@app.post('/achieved_cleaning')
async def predict_stage(d: List[Achieved]):
    da = []
    for i in d:
        da.append(dict(i))
    df = pd.DataFrame(data=da)

    df.drop(['og_id', 'download_status', 'organization_id', 'date', 'timestamp', 'created',
             'updated_at', 'approved_date', 'comment', 'data_transfer',
             'transfer_note', 'update_22_02_2021', 'sale_month_changed',
             'sub_category_id', 'lock_status', 'account_number', 'amount',
             'server_date'], axis=1, inplace=True)

    # function to convert empty strings to nan
    def empty_nan(df, col):
        df.loc[(df[col] == ''), col] = np.nan
        return df

    cat_columns = df.select_dtypes(['O']).columns
    for el in cat_columns:
        df = empty_nan(df, el)

    # converting month to corresponding numbers
    df['month'] = pd.to_datetime(df.month, format='%B').dt.month

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
                "name": 1,
                "zone_id": 1,

            }}
         ])

    # converting to dataframe
    user = pd.DataFrame(list(result))
    user.rename(columns={'name': 'user_name'}, inplace=True)

    df = df.merge(user, on="user_id", how="outer")

    # creating manager_name
    # fetching  user data
    result = client['saraswat']['users'].aggregate(
        [{
            "$addFields":
            {
                "manager_id": {"$toString": "$_id"}
            }},
            {
            "$project":
            {
                "_id": 0,
                "manager_id": 1,
                "name": 1,

            }}
         ])

    # converting to dataframe
    user = pd.DataFrame(list(result))
    user.rename(columns={'name': 'manager_name'}, inplace=True)
    df = df.merge(user, on="manager_id", how="outer")

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

    df = df.drop('zone_id', axis=1)

    # Adding a product_name column

    # fetching  product data
    result = client['saraswat']['product'].aggregate(
        [{
            "$addFields":
            {
                "product_id": {"$toString": "$_id"}
            }},
            {
            "$project":
            {
                "_id": 0,
                "product_id": 1,
                "product_name": 1,
                "product_category_id": 1,
                "product_main_category_id": 1,

            }}
         ])

    # converting to dataframe
    product = pd.DataFrame(list(result))

    df = df.merge(product, on="product_id", how="outer")

    # remove empty string
    df = empty_nan(df, 'product_name')
    df = empty_nan(df, 'product_category_id')

    # fetching product_category data
    result = client['saraswat']['product_category'].aggregate(
        [{
            "$addFields":
            {
                "product_category_id": {"$toString": "$_id"}
            }},
            {
            "$project":
            {
                "_id": 0,
                "product_category_id": 1,
                "category_name": 1
            }}
         ])

    # converting to dataframe
    product_category = pd.DataFrame(list(result))

    product_category.rename(
        columns={'category_name': 'product_category_name'}, inplace=True)
    df = df.merge(product_category, on="product_category_id", how="outer")

    df = empty_nan(df, 'product_category_name')

    # fetching product_main_category data
    result = client['saraswat']['product_main_category'].aggregate(
        [{
            "$addFields":
            {
                "product_main_category_id": {"$toString": "$_id"}
            }},
            {
            "$project":
            {
                "_id": 0,
                "product_main_category_id": 1,
                "category_name": 1
            }}
         ])

    # converting to dataframe
    product_main_category = pd.DataFrame(list(result))

    product_main_category.rename(
        columns={'category_name': 'product_main_category_name'}, inplace=True)
    df = df.merge(product_main_category,
                  on="product_main_category_id", how="outer")

    df = empty_nan(df, 'product_main_category_name')

    # dropping rows where user_id is null
    df.dropna(subset=['achieved'], inplace=True)

    # setting achieved against CASA and CREDIT_CARD to 1
    df.loc[df['product_main_category_name'] == 'CASA', 'achieved'] = 1
    df.loc[df['product_main_category_name'] == 'CREDIT_CARD', 'achieved'] = 1

    # Adding Mongodb column
    df.reset_index(drop=True, inplace=True)
    df_dict = df.to_dict('records')
    client.saraswat.da_achieved.insert_many(df_dict)
