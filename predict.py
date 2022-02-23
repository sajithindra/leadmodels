from fastapi import FastAPI
from pydantic import BaseModel, Field
from typing import List
from pydantic import validator
from bson import ObjectId
import leadscoremodule as ls
from datetime import datetime
import pickle
import pandas as pd
import joblib
import numpy as np
from pymongo import MongoClient

from typing import Optional
from bson import ObjectId

import zonewisemodule as zonewise
import cleaningmodules as clean
import productwisemodule as productmain
import subcategorymodule as productsub
import profile_quality
import json


# mongo client

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

###########  Lead data model #################################


class Lead(BaseModel):
    id: str

########### User data model #################################


class User(BaseModel):
    user_id: str

########### Product data model ############################


class Product(BaseModel):
    product_id: str

###########  Achieved data model #################################


class Achieved(BaseModel):
    achieved_id: str

###########  Target data model #################################


class Target(BaseModel):
    target_id: str

################### Sales data model for monthly predictions #################################


class Sales_Month(BaseModel):
    year: int
    month: int

################### Sales data model for yearly predictions #################################


class Sales_Year(BaseModel):
    year: int


app = FastAPI()
@app.get('/')
async def test():
    return {'404': 'Well done , you made it this far'}

########### predict stage model #########################
@app.post('/predictstage')
async def predict_stage(d: List[Lead]):
    rdata = []
    for i in d:

        client = MongoClient(
            'mongodb://localhost:27017/?readPreference=primary&appname=MongoDB+Compass&directConnection=true&ssl=false')
        filter = {
            'id': i.id
        }
        project = {
            '_id': 0,
            'profession': 1,
            'location': 1,
            'lead_status': 1,
            'lead_stage': 1,
            'company_name': 1,
            'leadsourcename': 1,
            'team_name': 1,
            'zone_name': 1,
            'product_category_name': 1,
            'product_main_category_name': 1
        }

        result = client['saraswat']['da_lead'].find_one(
            filter=filter,
            projection=project
        )
        df = pd.DataFrame([dict(result)])

        error_list = df.columns[df.isnull().all()].tolist()

        if len(error_list) > 0:
            return {"Error": f' {error_list} values are missing'}

        else:

            # Loading label encoders for each feature
            file = open("./notebook/profession_encoder.pkl", "rb")
            profession_encoder = pickle.load(file)

            file = open("./notebook/location_encoder.pkl", "rb")
            location_encoder = pickle.load(file)

            file = open("./notebook/lead_status_encoder.pkl", "rb")
            lead_status_encoder = pickle.load(file)

            file = open("./notebook/company_encoder.pkl", "rb")
            company_encoder = pickle.load(file)

            file = open("./notebook/lead_stage_encoder.pkl", "rb")
            lead_stage_encoder = pickle.load(file)

            file = open("./notebook/lead_source_encoder.pkl", "rb")
            lead_source_encoder = pickle.load(file)

            file = open("./notebook/team_name_encoder.pkl", "rb")
            team_name_encoder = pickle.load(file)

            file = open("./notebook/zone_name_encoder.pkl", "rb")
            zone_name_encoder = pickle.load(file)

            file = open("./notebook/prod_cat_encoder.pkl", "rb")
            prod_cat_encoder = pickle.load(file)

            file = open("./notebook/prod_main_encoder.pkl", "rb")
            prod_main_encoder = pickle.load(file)

            profession_encoded = profession_encoder.transform(df['profession'])
            location_encoded = location_encoder.transform(df['location'])
            company_encoded = company_encoder.transform(df['company_name'])
            lead_status_encoded = lead_status_encoder.transform(
                df['lead_status'])
            lead_source_encoded = lead_source_encoder.transform(
                df['leadsourcename'])
            team_name_encoded = team_name_encoder.transform(df['team_name'])
            zone_name_encoded = zone_name_encoder.transform(df['zone_name'])
            prod_cat_encoded = prod_cat_encoder.transform(
                df['product_category_name'])
            prod_main_encoded = prod_main_encoder.transform(
                df['product_main_category_name'])

            feat = list(zip(profession_encoded, location_encoded, company_encoded, lead_status_encoded,
                            lead_source_encoded, team_name_encoded, zone_name_encoded, prod_cat_encoded, prod_main_encoded))

            model = joblib.load('./notebook/leadstage.pkl')

            stage = model.predict(feat)

            result = lead_stage_encoder.inverse_transform(stage)[0]

            return {"id": i.id, 'Lead Stage': result}
#############  customer prediction API #################################


@app.post('/customer_profile')
async def customer_profile(d: List[Lead]):
    rdata = []
    for i in d:
        filter = {
            '_id': ObjectId(i.id)
        }
        project = {
            'profession': 1,
            '_id': 0
        }

        result = client['saraswat']['lead'].find_one(
            filter=filter,
            projection=project
        )

        # Loading one hot encoder
        file = open("./notebook/onehot_profession_encoder.pkl", "rb")
        profession_encoder = pickle.load(file)

        # loading model
        file = open("./notebook/rf_customer_smote.pkl", "rb")
        model = pickle.load(file)
        data = pd.DataFrame([dict(result)])

        # empty dataframe and empty string
        if data.empty == True:
            return 'lead id not valid'
        elif data[data['profession'] == ''].empty == False:
            return 'profession not valid'
        else:
            profession_encoded = profession_encoder.transform(
                data[['profession']])

            prediction = model.predict(profession_encoded)

            if prediction[0] == 1:
                profile = 'high'
            else:
                profile = 'low'
            rdata.append({"id": i.id, "Customer Profile": profile})

    return rdata
########################################################################


############### Lead data model #################################


###############   Lead quality prediction API  #######################################

@app.post('/lead_quality')
async def predict_quality(d: List[Lead]):
    rdata = []
    for i in d:
        filter = {
            'id': i.id
        }
        project = {
            '_id': 0,
            'id': 1,
            'profession': 1,
            'location': 1,
            'lead_status': 1,
            'emails': 1,
            'phones': 1,
            'project_id': 1,
            'company_name': 1,
            'products': 1,
            'lead_stage': 1,
            'lead_source': 1,

        }

        result = client['saraswat']['da_lead'].find_one(
            filter=filter,
            projection=project
        )

        lead = dict(result)

        # getting customer_profile
        # Loading one hot encoder
        file = open("./notebook/onehot_profession_encoder.pkl", "rb")
        profession_encoder = pickle.load(file)

        # loading model
        file = open("./notebook/rf_customer_smote.pkl", "rb")
        profile_model = pickle.load(file)

        if lead['profession'] == '' or str(lead['profession']) == 'nan':
            lead_quality = 'Not enough data'
        else:

            profession_encoded = profession_encoder.transform(
                [[lead['profession']]])

            prediction = profile_model.predict(profession_encoded)

            if prediction[0] == 1:
                customer_profile = 'high'
            else:
                customer_profile = 'low'

            lead['customer_profile'] = customer_profile

            count = len(dict((key, value)
                             for key, value in lead.items() if str(value) != 'nan'))

            # loading encoders
            file = open("./notebook/customer_profile_encoder.pkl", "rb")
            customer_profile_encoder = pickle.load(file)

            file = open("./notebook/lead_quality_encoder.pkl", "rb")
            lead_quality_encoder = pickle.load(file)

            file = open("./notebook/rf_smote_lead_quality.pkl", "rb")
            model = pickle.load(file)

            profile_encoded = customer_profile_encoder.transform(
                [lead['customer_profile']])

            X = np.asarray([[count, profile_encoded[0]]])

            quality = model.predict(X)
            lead_quality = lead_quality_encoder.inverse_transform(quality)[0]

        rdata.append({"id": i.id, "lead quality": lead_quality})

    return rdata


@app.post('/predictsales_year_amount')
async def predict_stage(d: List[Sales_Year]):

    rdata = []
    for i in d:

        year = i.year

        n_periods = year - 2021

        file = open("./notebook/arima_allzone_amount_year.pkl", "rb")
        model = pickle.load(file)

        df = pd.DataFrame(model.predict(n_periods=n_periods))
        pred_list = df[0].to_list()
        pred_value = round(pred_list[n_periods-1])
        rdata.append({"year": i.year, "sales": pred_value})
    return rdata


@app.post('/predictsales_year_number')
async def predict_stage(d: List[Sales_Year]):

    rdata = []
    for i in d:
        year = i.year

        n_periods = year - 2021

        file = open("./notebook/arima_allzone_num_year.pkl", "rb")
        model = pickle.load(file)

        df = pd.DataFrame(model.predict(n_periods=n_periods))
        pred_list = df[0].to_list()
        pred_value = round(pred_list[n_periods-1])
        rdata.append({"year": i.year, "sales": pred_value})
    return rdata


@app.post('/predictsales_month_amount')
async def predict_stage(d: List[Sales_Month]):

    rdata = []
    for i in d:
        year = i.year

        month = i.month

        n_year = year - 2021

        # num of months from dec 2021 till the input month
        n_periods = (n_year * month)+1
        file = open("./notebook/arima_allzone_amount_month.pkl", "rb")
        model = pickle.load(file)

        df = pd.DataFrame(model.predict(n_periods=n_periods))
        pred_list = df[0].to_list()
        pred_value = round(pred_list[n_periods-1])
        rdata.append({"year": i.year, "month": i.month, "sales": pred_value})
    return rdata


@app.post('/predictsales_month_number')
async def predict_stage(d: List[Sales_Month]):

    rdata = []
    for i in d:

        year = i.year

        month = i.month

        n_year = year - 2021

        # num of months from dec 2021 till the input month
        n_periods = (n_year * month)+1
        file = open("./notebook/arima_allzone_num_month.pkl", "rb")
        model = pickle.load(file)

        df = pd.DataFrame(model.predict(n_periods=n_periods))
        pred_list = df[0].to_list()
        pred_value = round(pred_list[n_periods-1])
        rdata.append({"year": i.year, "month": i.month, "sales": pred_value})
    return rdata


################################ Lead og data set #################################


@app.post('/leadscore')
async def lead_score(d: List[Lead]):
    rdata = []
    count = 0
    for i in d:
        count = client.saraswat.lead.count_documents(
            {"_id": ObjectId(i.id)})

        if count == 0:
            rdata.append({"id": i.id, "Error": "Lead is not in the database."})
        else:

            result = client['saraswat']['lead'].aggregate([
                {

                    '$match': {'_id': ObjectId(i.id)}

                },

                {
                    '$addFields': {

                        'products': '$products.id',

                    }
                }, {
                    '$addFields': {

                        'products': {
                            '$arrayElemAt': [
                                '$products', 0
                            ]
                        }

                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'profession': 1,
                        'location': 1,
                        'lead_status': 1,
                        'lead_source': 1,
                        'created': 1,
                        'user_id': 1,  # to fetch zone_name
                        'products': 1



                    }
                }
            ])
            data = list(result)[0]

            # fetching product_name
            filter = {
                '_id': ObjectId(data['products'])
            }
            project = {
                '_id': 0,
                'product_name': 1,
                'product_category_id': 1,
                'product_main_category_id': 1
            }

            result = client['saraswat']['product'].find_one(
                filter=filter,
                projection=project
            )
            product = dict(result)

            # function to fetch values from other collections
            def fetch_value(oid, field, collection):

                filter = {
                    '_id': ObjectId(oid)
                }
                project = {
                    '_id': 0,
                    field: 1,

                }

                result = client['saraswat'][collection].find_one(
                    filter=filter,
                    projection=project
                )
                return dict(result)

            # fetching values
            data['product_name'] = product['product_name']
            users = fetch_value(data['user_id'], 'zone_id', 'users')
            data['zone_name'] = fetch_value(
                users['zone_id'], 'name', 'zone')['name']
            data['product_category_name'] = fetch_value(
                product['product_category_id'], 'category_name', 'product_category')['category_name']
            data['product_main_category_name'] = fetch_value(
                product['product_main_category_id'], 'category_name', 'product_main_category')['category_name']
            data['lead_source'] = fetch_value(
                data['lead_source'], 'name', 'lead_source')['name']

            # deleting unwanted key value pairs
            del data["user_id"]
            del data["products"]

            leadscore = 0
            #print (dict(result))
            for j in data.keys():
                leadscore += ls.lspull(j, data[j])

        created = datetime.strptime(data['created'], '%Y-%m-%dT%H:%M:%S')
        time = datetime.now()-created
        if time.days < 30:
            leadscore += 10
        rdata.append({"id": i.id, "leadscore": leadscore})
    return rdata

################################ high quality lead followup count ########################


@app.post('/high_quality_lead_followup__count')
async def lead_count(d: List[Lead]):
    rdata = []
    count = 0
    doc_count = 0

    for i in d:

        doc_count = client.saraswat.lead.count_documents(
            {"_id": ObjectId(i.id)})

        if doc_count == 0:
            rdata.append({"id": i.id, "Error": "Lead is not in the database."})
        else:
            profile = profile_quality.customer_profile(i.id)
            lead_quality = profile_quality.lead_quality(i.id, profile)

            count += profile_quality.high_followup_count(i.id, lead_quality)

    rdata.append({"count": count})

    return rdata


################################ zonewise sales forecast #################################


####### Zone I #######

@app.post('/predictsales_zone1_year_number')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = zonewise.sales_forecast_year_num('Zone I', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_zone1_month_number')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = zonewise.sales_forecast_month_num('Zone I', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata

####### Zone II #######


@app.post('/predictsales_zone2_year_number')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = zonewise.sales_forecast_year_num('Zone II', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_zone2_month_number')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = zonewise.sales_forecast_month_num('Zone II', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata


####### Zone III #######

@app.post('/predictsales_zone3_year_number')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = zonewise.sales_forecast_year_num('Zone III', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_zone3_month_number')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = zonewise.sales_forecast_month_num('Zone III', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata

####### Zone IV #######


@app.post('/predictsales_zone4_year_number')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = zonewise.sales_forecast_year_num('Zone IV', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_zone4_month_number')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = zonewise.sales_forecast_month_num('Zone IV', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata


################################ data Cleaning #################################

####### Lead #######

@app.post('/lead_cleaning')
async def lead_cleaning(d: List[Lead]):
    rdata = []
    count = 0
    for i in d:
        count = client.saraswat.lead.count_documents(
            {"_id": ObjectId(i.id)})

        if count == 0:
            rdata.append({"id": i.id, "Error": "Lead is not in the database."})
        else:

            clean.lead_cleaning(i.id)

            rdata.append({"id": i.id, "Status": 'Successful'})
    return rdata


####### Achieved #######

@app.post('/achieved_cleaning')
async def achieved_cleaning(d: List[Achieved]):
    rdata = []
    count = 0
    for i in d:
        count = client.saraswat.achieved.count_documents(
            {"_id": ObjectId(i.achieved_id)})

        if count == 0:
            rdata.append(
                {"id": i.achieved_id, "Error": "Achieved Id is not in the database."})
        else:

            clean.achieved_cleaning(i.achieved_id)

            rdata.append({"id": i.achieved_id, "Status": 'Successful'})
    return rdata

####### Target #######


@app.post('/target_cleaning')
async def target_cleaning(d: List[Target]):
    rdata = []
    count = 0
    for i in d:
        count = client.saraswat.target.count_documents(
            {"_id": ObjectId(i.target_id)})

        if count == 0:
            rdata.append(
                {"id": i.target_id, "Error": "Target Id is not in the database."})
        else:

            clean.target_cleaning(i.target_id)

            rdata.append({"id": i.target_id, "Status": 'Successful'})
    return rdata

####### Target #######


@app.post('/user_cleaning')
async def user_cleaning(d: List[User]):
    rdata = []
    count = 0
    for i in d:
        count = client.saraswat.users.count_documents(
            {"_id": ObjectId(i.user_id)})

        if count == 0:
            rdata.append(
                {"id": i.user_id, "Error": "User Id is not in the database."})
        else:

            clean.user_cleaning(i.user_id)

            rdata.append({"id": i.user_id, "Status": 'Successful'})
    return rdata


################################ product main category wise sales forecast #################################

####### Yearly #######

@app.post('/predictsales_advance_year')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = productmain.sales_forecast_year('ADVANCE', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_tpp_year')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = productmain.sales_forecast_year('TPP', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_ltsd_year')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = productmain.sales_forecast_year('LTSD/TD', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_credit_card_year')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = productmain.sales_forecast_year('CREDIT_CARD', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_casa_year')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = productmain.sales_forecast_year('CASA', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


####### Monthly #######

@app.post('/predictsales_advance_month')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = productmain.sales_forecast_month('ADVANCE', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata


@app.post('/predictsales_tpp_month')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = productmain.sales_forecast_month('TPP', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata


@app.post('/predictsales_ltsd_month')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = productmain.sales_forecast_month('LTSD/TD', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata


@app.post('/predictsales_credit_card_month')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = productmain.sales_forecast_month(
            'CREDIT_CARD', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata


@app.post('/predictsales_casa_month')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = productmain.sales_forecast_month('CASA', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata


################################ product sub category wise sales forecast #################################

####### Yearly #######

@app.post('/predictsales_tab_banking1_year')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = productsub.sales_forecast_year('TAB BANKING-I', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_current_year')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = productsub.sales_forecast_year('Current', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_savings_year')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = productsub.sales_forecast_year('Savings', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_creditcard_year')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = productsub.sales_forecast_year('CREDIT CARD', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_general_insurance_year')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = productsub.sales_forecast_year('General Insurance', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_health_insurance_year')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = productsub.sales_forecast_year('Health Insurance', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_life_insurance_year')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = productsub.sales_forecast_year('Life Insurance', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_mutual_fund_lump_sum_year')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = productsub.sales_forecast_year(
            'Mutual Fund (Lump Sum)', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_mutual_fund_sip_year')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = productsub.sales_forecast_year('Mutual Fund (SIP)', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_term_deposit_year')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = productsub.sales_forecast_year('Term Deposit', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_commercial_loan_year')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = productsub.sales_forecast_year('Commercial Loan', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_retail_year')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = productsub.sales_forecast_year('Retail', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata


@app.post('/predictsales_table_chart_year')
async def predict_sales(d: List[Sales_Year]):

    rdata = []
    for i in d:

        value = productsub.sales_forecast_year('TABLE CHART', i.year)

        rdata.append({"year": i.year, "sales": value})
    return rdata

####### Monthly #######


@app.post('/predictsales_tab_banking1_month')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = productsub.sales_forecast_month(
            'TAB BANKING-I', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata


@app.post('/predictsales_current_month')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = productsub.sales_forecast_month('Current', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata


@app.post('/predictsales_savings_month')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = productsub.sales_forecast_month('Savings', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata


@app.post('/predictsales_creditcard_month')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = productsub.sales_forecast_month(
            'CREDIT CARD', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata


@app.post('/predictsales_general_insurance_month')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = productsub.sales_forecast_month(
            'General Insurance', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata


@app.post('/predictsales_health_insurance_month')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = productsub.sales_forecast_month(
            'Health Insurance', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata


@app.post('/predictsales_life_insurance_month')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = productsub.sales_forecast_month(
            'Life Insurance', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata


@app.post('/predictsales_mutual_fund_lumpsum_month')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = productsub.sales_forecast_month(
            'Mutual Fund (Lump Sum)', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata


@app.post('/predictsales_mutual_fund_sip_month')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = productsub.sales_forecast_month(
            'Mutual Fund (SIP)', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata


@app.post('/predictsales_term_deposit_month')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = productsub.sales_forecast_month(
            'Term Deposit', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata


@app.post('/predictsales_retail_month')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = productsub.sales_forecast_month(
            'Retail', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata


@app.post('/predictsales_commercial_loan_month')
async def predict_sales(d: List[Sales_Month]):

    rdata = []
    for i in d:
        value = productsub.sales_forecast_month(
            'Commercial Loan', i.year, i.month)
        rdata.append({"year": i.year, "month": i.month, "sales": value})
    return rdata
