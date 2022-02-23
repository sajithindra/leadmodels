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


class Lead(BaseModel):
    og_id: Optional[PyObjectId] = Field(alias='_id')
    client_name: str
    timestamp: str
    profession: str
    description: str
    lead_source: str
    leadsource_subcategory: str
    mo_leadsource_subcategory: str
    address: str
    location: str
    user_id: str
    lead_stage: str
    lead_status: str
    lead_id: str
    emails: list
    phones: list
    projects: list
    project_id: str
    expected_closedate: str
    expected_target: str
    organization_id: str
    status: int
    order: int
    company_name: str
    download_status: float
    created: str
    createdby: str
    updated_at: datetime.datetime
    created_at: datetime.datetime
    lead_sub_stage: str
    enquiry_id: str
    feedback: str
    leadsourcename: str
    expected_sales: str
    products: list
    sync_status: float
    assigned_date: str
    assigned_timestamp: float
    zone_id: str
    product_id: str
    close_date: str
    failed_reason: str
    lead_delay_flag: float
    leadreassigned_state: str
    purgone: str
    purgtwo: str
    real_purg: str
    purged_lead: str
    attended_lead: float
    data_transfer: str
    transfer_note: str
    contact_person: str
    created_from: str
    lead_failed_reason: str
    manager_id: str
    pin_code: str
    duplicate_status: float
    srno: str
    date_on_lead_sent: str
    branch_code: str
    alternate_mobile_no: str
    lead_sent_by: str
    zbdo_name: str
    assigned_to_bdo: str
    remarks_1st_conversation: str
    remarks_further_conversation_should_be_date_wise__if_loan_is_disbursed_pls_mention_product_loan_account_no: str
    follow_up_pending_at_below_stage: str
    zero: str = Field(alias='0')
    lead_source_subcategory: str
    upload_timestamp: str
    remarks_further_conversation_should_be_date_wise: str
    reason: str
    residential_status: str
    cbs_closed_date: str
    alternate_no: str
    stage: str
    remarks_further_conversation_should_be_date_wise_if_loan_is_disbursed_pls_mention_product_loan_account_no: float
    datetime: str
    temp_id: str
    lock_status: str
    id_drop: str = Field(alias='id')
    delete_lead_download_status: float
    zbdc_name: str
    branch_name: str
    name_of_the_customer: str
    type_of_loan: str
    amount_in_lakhs: str
    remarks_further_conversation_should_be_date_wise__if_loan_is_disbursed_pls_mention_product_and_loan_account_no: str = Field(
        alias='remarks_further_conversation_should_be_date_wise__if_loan_is_disbursed_pls_mention_product_&_loan_account_no')
    main_id: str
    f: float
    amount: str
    one_pager: str
    one_pager_file: str

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            ObjectId: str
        }

    @validator('lead_status')
    def check_status(cls, value):
        if value not in ['Hot', 'Warm', 'Converted', 'Cold']:
            raise ValueError('lead_status value not valid')
        return value

    @validator('lead_stage')
    def check_stage(cls, value):
        if value not in ['5d81b3a6685488236e5d7243', '5df1fbce0fa71329e2594e98',
                         '5d66731068548872f269fbf3', '5d81b6826854883c657145f3',
                         '5d6672f868548871d03908d3', '5df1fdc6c82cd219f4ff32a5',
                         '5df1fd3bc82cd219f4ff32a0']:
            raise ValueError('lead_stage value not valid')
        return value

    @validator('client_name')
    def check_client_name(cls, value):
        if value == "":
            raise ValueError('Client name is empty')
        elif value.isdigit():
            raise ValueError('Client name is not valid')
        return value

    @validator('profession')
    def check_profession(cls, value):
        assert value.isalpha(), 'not valid'
        return value

    @validator('status')
    def check_status(cls, value):
        if value not in [0, 1]:
            raise ValueError('status not valid')
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

    @validator('purged_lead')
    def check_purgedlead(cls, value):
        if value not in ['0', '1', 0]:
            raise ValueError('status not valid')
        return value

    @validator('pin_code', 'branch_code')
    def check_numeric(cls, value):
        assert value.isnumeric(), 'must be numeric'
        return value

    @validator("emails")
    def check_email(cls, value):

        def string_extract(element, key):
            return ",".join(element[i][key] for i in range(len(element)))

        email = string_extract(value, 'email')

        if ' ' in email:
            raise ValueError("email not valid")
        if "@" not in email:
            raise ValueError("email not valid")
        name, domain = value.split('@', 1)
        if '.' not in domain[1:]:
            raise ValueError("email not valid")
        return value

    @validator('project_id', 'product_id', 'organization_id')
    def check_ids(cls, value):
        assert value.isalnum(), 'must be alphanumeric'
        return value


app = FastAPI()
@app.get('/')
async def test():
    return {'404': 'Test was sucessful'}


@app.post('/lead_cleaning')
async def predict_stage(d: List[Lead]):
    da = []
    for i in d:
        da.append(dict(i))
    lead = pd.DataFrame(data=da)

    drop_list = ['remarks_further_conversation_should_be_date_wise', 'alternate_no', 'reason', 'follow_up_pending_at_below_stage', 'zero', 'amount_in_lakhs', 'id_drop', 'main_id', 'purgtwo', 'remarks_further_conversation_should_be_date_wise__if_loan_is_disbursed_pls_mention_product_and_loan_account_no', 'lock_status', 'f', 'purgone', 'lead_sent_by', 'remarks_further_conversation_should_be_date_wise__if_loan_is_disbursed_pls_mention_product_loan_account_no', 'mo_leadsource_subcategory', 'amount', 'assigned_timestamp', 'duplicate_status', 'type_of_loan',
                 'remarks_1st_conversation', 'remarks_further_conversation_should_be_date_wise_if_loan_is_disbursed_pls_mention_product_loan_account_no', 'data_transfer', 'transfer_note', 'name_of_the_customer', 'contact_person', 'srno', 'date_on_lead_sent', 'real_purg', 'stage', 'delete_lead_download_status', 'projects', 'assigned_to_bdo', 'timestamp', 'order', 'download_status', 'updated_at', 'created_at', 'sync_status', 'assigned_timestamp', 'assigned_date', 'alternate_mobile_no', 'lead_delay_flag', 'lead_failed_reason', 'lead_source_subcategory', 'attended_lead']
    lead.drop(drop_list, axis=1, inplace=True)

    # function to convert empty strings to nan
    def empty_nan(df, col):
        df.loc[(df[col] == ''), col] = np.nan
        return df

    # defining function to extract values from list of dictionary
    def string_extract(element, key):
        return ",".join(element[i][key] for i in range(len(element)))

    # defining function to return only numeric values

    def numeric(element):
        return "".join(item for item in str(element) if item.isdigit()).strip()

    # renaming column
    lead.rename(columns={'og_id': 'id'}, inplace=True)

    # converting ObjectId to string
    lead['id'] = lead['id'].values.astype(str)

    # emails
    lead.loc[:, 'emails'] = [string_extract(
        x, 'email') for x in lead['emails'].values]
    lead = empty_nan(lead, 'emails')

    # phones
    lead.loc[:, 'phones'] = [string_extract(
        x, 'phone') for x in lead['phones'].values]

    # products
    lead.loc[:, 'products'] = [string_extract(
        x, 'id') for x in lead['products'].values]
    lead = empty_nan(lead, 'products')

    # for dropping product rows with multiple entries
    lead = lead.loc[~(lead['products'].str.len() > 24)]

    # extracting only id from lead_id
    lead.loc[:, 'lead_id'] = [numeric(x) for x in lead['lead_id'].values]
    # replacing empty strings
    lead = empty_nan(lead, 'lead_id')

    # replacing empty string with nan
    lead = empty_nan(lead, 'expected_sales')
    # replacing empty string with nan
    lead = empty_nan(lead, 'expected_target')

    # converting dtype from Object to float
    lead['expected_sales'] = lead['expected_sales'].astype(float)
    lead['expected_target'] = lead['expected_target'].astype(float)

    # removing NoneType from columns
    lead.zbdc_name.fillna(value=np.nan, inplace=True)

    # replacing NaN with 0 in purged_lead
    lead['purged_lead'] = lead['purged_lead'].fillna(0)

    # converting purged_lead dtype to int
    lead['purged_lead'] = lead['purged_lead'].astype("int")

    # lead_stage

    # fetching lead_stage data
    result = client['saraswat']['lead_stage'].aggregate(
        [{
            "$addFields":
            {
                "lead_stage": {"$toString": "$_id"}

            }},
            {
            "$project":
            {
                "_id": 0,
                "stage": 1,
                "lead_stage": 1

            }}
         ])

    # converting to dataframe
    lead_stage = pd.DataFrame(list(result))

    lead = lead.merge(lead_stage, on='lead_stage', how='outer')

    lead = lead.drop('lead_stage', axis=1)
    lead.rename(columns={'stage': 'lead_stage'}, inplace=True)

    # lead_source

    # fetching  data

    result = client['saraswat']['lead_source'].aggregate(
        [{
            "$addFields":
            {
                "lead_source": {"$toString": "$_id"}
            }},
            {
            "$project":
            {
                "_id": 0,
                "lead_source": 1,
                "name": 1
            }}
         ])

    # converting to dataframe
    source = pd.DataFrame(list(result))

    lead = lead.merge(source, on='lead_source', how='outer')

    lead = lead.drop('lead_source', axis=1)
    lead.rename(columns={'name': 'lead_source'}, inplace=True)

    # replacing empty strings
    lead = empty_nan(lead, 'lead_source')

    # replacing top500 with nan
    lead.loc[lead['lead_source'].str.contains(
        "Top500", na=False), 'lead_source'] = np.nan

    # lead_sub_stage

    # fetching  data

    result = client['saraswat']['lead_sub_stage'].aggregate(
        [{
            "$addFields":
            {
                "lead_sub_stage": {"$toString": "$_id"}
            }},
            {
            "$project":
            {
                "_id": 0,
                "lead_sub_stage": 1,
                "name": 1
            }}
         ])

    # converting to dataframe
    lstage = pd.DataFrame(list(result))
    lead = lead.merge(lstage, on='lead_sub_stage', how='outer')

    lead = lead.drop('lead_sub_stage', axis=1)
    lead.rename(columns={'name': 'lead_sub_stage'}, inplace=True)

    # failed_reason

    # fetching  failed_reason data
    result = client['saraswat']['failed_reason'].aggregate(
        [{
            "$addFields":
            {
                "failed_reason": {"$toString": "$_id"}
            }},
            {
            "$project":
            {
                "_id": 0,
                "failed_reason": 1,
                "name": 1
            }}
         ])

    # converting to dataframe
    f_reasondf = pd.DataFrame(list(result))

    lead = lead.merge(f_reasondf, on='failed_reason', how='outer')

    lead = lead.drop('failed_reason', axis=1)
    lead.rename(columns={'name': 'failed_reason'}, inplace=True)

    # failed_reason
    lead = empty_nan(lead, 'failed_reason')

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
                "user_type": 1,
                "employee_id": 1,
                'phone': 1,
                "role_id": 1,
                "team": 1,
                "email": 1,
                "status": 1
            }}
         ])

    # converting to dataframe
    user = pd.DataFrame(list(result))
    user.rename(columns={'phone': 'user_phone', 'email': 'user_email',
                         'status': 'user_status', 'name': 'user_name'}, inplace=True)

    lead = lead.merge(user, on="user_id", how="outer")

    # Adding a team_name column

    # fetching  roles data
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
    lead = lead.merge(team, on="team", how="outer")

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

    lead = lead.merge(roles, on="role_id", how="outer")

    # replacing createdby with names
    # fetching  user data
    result = client['saraswat']['users'].aggregate(
        [{
            "$addFields":
            {
                "createdby": {"$toString": "$_id"}
            }},
            {
            "$project":
            {
                "_id": 0,
                "createdby": 1,
                "name": 1,

            }}
         ])

    # converting to dataframe
    user = pd.DataFrame(list(result))
    user.rename(columns={'name': 'created_name'}, inplace=True)
    lead = lead.merge(user, on="createdby", how="outer")
    lead = lead.drop('createdby', axis=1)
    lead.rename(columns={'created_name': 'createdby'}, inplace=True)

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
    lead = lead.merge(user, on="manager_id", how="outer")

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

    lead = lead.merge(zone, on="zone_id", how="outer")

    # Adding a project_name and product name column

    # fetching  product data
    result = client['saraswat']['product'].aggregate(
        [{
            "$addFields":
            {
                "products": {"$toString": "$_id"}
            }},
            {
            "$project":
            {
                "_id": 0,
                "products": 1,
                "name": 1,
                "product_category_id": 1,
                "product_main_category_id": 1,
                "status": 1

            }}
         ])

    # converting to dataframe
    product = pd.DataFrame(list(result))

    project = product[['name', 'products']]

    project.rename(columns={'name': 'project_name',
                            'products': 'project_id'}, inplace=True)
    lead = lead.merge(project, on="project_id", how="outer")

    product.rename(columns={'name': 'product_name',
                            'status': 'product_status'}, inplace=True)
    lead = lead.merge(product, on="products", how="outer")

    # removing empty strings
    # project_name
    lead = empty_nan(lead, 'project_name')
    # product_name
    lead = empty_nan(lead, 'product_name')
    # product_category_id
    lead = empty_nan(lead, 'product_category_id')
    # product_main_category_id
    lead = empty_nan(lead, 'product_main_category_id')

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
    lead = lead.merge(product_category, on="product_category_id", how="outer")

    lead = empty_nan(lead, 'product_category_name')

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
    lead = lead.merge(product_main_category,
                      on="product_main_category_id", how="outer")

    lead = empty_nan(lead, 'product_main_category_name')

    # Adding year and month column
    lead['year'] = pd.DatetimeIndex(lead['created']).year
    lead['month'] = pd.DatetimeIndex(lead['created']).month

    # dropping rows where  lead_stage is nan
    lead.dropna(subset=['lead_stage'], inplace=True)

    cat_columns = lead.select_dtypes(['O']).columns
    for el in cat_columns:
        lead = empty_nan(lead, el)

    # Adding collection to Mongodb
    lead.reset_index(drop=True, inplace=True)

    lead_dict = lead.to_dict('records')
    client.saraswat.da_lead.insert_many(lead_dict)
