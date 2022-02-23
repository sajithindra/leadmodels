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


class User(BaseModel):
    user_id:  Optional[PyObjectId] = Field(alias='_id')
    first_name: str
    phone: str
    email: str
    address: str
    user_type: str
    sub_domain: str
    organization: dict
    password: str
    remaining_licence: str
    number_of_license: str
    status: int
    updated_at: datetime.datetime
    remember_token: str
    forgot_token: str
    settings_status: str
    logo: str
    file: str
    login_key: str
    pdf_footer: str
    pdf_header: str
    org_code: str
    auto_punchout: str
    autosync_time: str
    currency_type: str
    expense_approval: str
    modules: dict
    month_option: str
    name: str
    org_exp_date: str
    rate_field: str
    shop_location: str
    state_option: list
    target: str
    target_fields: str
    target_type: str
    time_zone: str
    tracking_time: str
    transportation_mode: str
    organization_id: str
    role_id: str
    fcm_token: str
    download_status: str
    productsubcategory_download_status: str
    product_download_status: str
    product_subcategory_download_status: str
    failed_stage_download_status: str
    source_download_status: str
    subsource_download_status: str
    reassign_user_download_status: str
    leaves: str
    checkin: str
    productcategory_download_status: str
    employee_id: str
    zone: str
    manager_id: str
    team: str
    organization_code: str
    otp: str
    punch_status: str
    created: str
    activity_download_status: str
    sales_download_status: str
    lead_download_status: str
    project_download_status: str
    GCM: str
    all_activity_download_status: str
    all_lead_download_status: str
    all_lead_notes_download_status: str
    all_sales_download_status: str
    block_download_status: str
    carblock_download_status: str
    delete_activity_download_status: str
    delete_lead_download_status: str
    delete_sales_download_status: str
    lead_note_download_status: str
    punch_time: str
    mo_manager_id: dict
    mo_team: dict
    zone_id: str
    loan_category_download_status: str
    expense_category_download_status: str
    leave_category_download_status: str
    target_download_status: str
    old_employee_id: str
    product_category_download_status: str
    reference_download_status: str
    stage_download_status: str
    sub_stage_download_status: str
    v2_update: str
    v4_update: str
    FCM: str
    email_id: str
    download_status_flag: str
    device: list
    image: str
    auto_sync_status: str
    device: list
    edit_img: str
    emailold: str
    salary: str
    phone2: str
    last_name: str
    last_Name: str
    zone_download_status: str
    job_title: str
    org_id: str
    product_main_category_id: str
    brand_id: str
    district: str
    region_id: str
    user_id: str
    newuser: str

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            ObjectId: str
        }

    @validator('user_type')
    def check_user_type(cls, value):
        if value not in ['organization', 'Admin Manager', 'Bpc', 'superadmin', 'Asm']:
            raise ValueError('user_type value not valid')
        return value

    @validator('status')
    def check_status(cls, value):
        if value not in [0, 1]:
            raise ValueError('status not valid')
        return value

    @validator('role_id')
    def check_role_id(cls, value):
        if value not in ['5d5fccd634bfbc7e2902eab2', '5d5fccd634bfbc7e2902eab3',
                         '5d5fccd634bfbc7e2902eab4']:
            raise ValueError('role_id not valid')
        return value

    @validator('user_id', 'manager_id')
    def check_ids(cls, value):
        assert value.isalnum(), 'must be alphanumeric'
        return value

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

    @validator("phone")
    def check_phone(cls, value):

        # phone number should typically be of length 10
        if value.isnumeric() == False:
            raise ValueError("phone not valid")
        return value

    @validator("email")
    def check_email(cls, value):

        if ' ' in value:
            return value
        if "@" not in value:
            raise ValueError("email not valid")
        name, domain = value.split('@', 1)
        if '.' not in domain[1:]:
            raise ValueError("email not valid")
        return value


app = FastAPI()
@app.get('/')
async def test():
    return {'404': 'Test was sucessful'}


@app.post('/user_cleaning')
async def predict_stage(d: List[User]):
    da = []
    for i in d:
        da.append(dict(i))
    df = pd.DataFrame(data=da)

    user = df[['user_id', 'name', 'user_type', 'employee_id', 'phone', 'manager_id', 'email',
               'team', 'organization_code', 'organization', 'sub_domain', 'role_id', 'zone_id', 'status']]

    # function to convert empty strings to nan
    def empty_nan(df, col):
        df.loc[(df[col] == ''), col] = np.nan
        return df

    # converting empty strings to nan
    cat_columns = user.select_dtypes(['O']).columns
    for el in cat_columns:
        user = empty_nan(user, el)

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

    user = user.merge(zone, on="zone_id", how="outer")

    # Adding a role_name column

    # fetching  roles data
    result = client['saraswat']['roles'].aggregate(
        [{
            "$addFields":
            {
                "role_id": {"$toString": "$_id"}
            }},
            {
            "$project":
            {
                "_id": 0,
                "role_id": 1,
                "label": 1

            }}
         ])

    # converting to dataframe
    roles = pd.DataFrame(list(result))

    roles.rename(columns={'label': 'role_name'}, inplace=True)
    user = user.merge(roles, on="role_id", how="outer")

    # Adding a team_name column

    # fetching  team data
    result = client['saraswat']['team'].aggregate(
        [{
            "$addFields":
            {
                "idt": {"$toString": "$_id"}
            }},
            {
            "$project":
            {
                "_id": 0,
                "idt": 1,
                "team": 1
            }}
         ])

    # converting to dataframe
    team = pd.DataFrame(list(result))

    team.rename(columns={'team': 'team_name', 'idt': 'team'}, inplace=True)
    user = user.merge(team, on="team", how="outer")

    user.loc[~(user['team'].str.len() == 24), 'team_name'] = user['team']

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
    user2 = pd.DataFrame(list(result))
    user2.rename(columns={'name': 'manager_name'}, inplace=True)
    user = user.merge(user2, on="manager_id", how="outer")

    # dropping rows where user_idis nan
    user.dropna(subset=['user_id'], inplace=True)

    # Adding collection to Mongodb
    user.reset_index(drop=True, inplace=True)
    userdict = user.to_dict('records')
    client.saraswat.da_user.insert_many(userdict)
