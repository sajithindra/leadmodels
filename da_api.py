from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
from pymongo import MongoClient


client = MongoClient(
    'mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false')


class dashboard(BaseModel):
    zone_name: str
    year: int
    month: int


app = FastAPI()
@app.get('/')
async def test():
    return {'404': 'Test was sucessful'}


@app.post('/leadstatus_count')
async def status(d: List[dashboard]):

    filter = {
        'zone_name': d[0].zone_name,
        'year': d[0].year,
        'month': d[0].month
    }
    project = {
        '_id': 0,
        'zone_name': 1,
        'lead_status': 1,
        'lead_status_count': 1
    }

    result = client['saraswat']['da_zone_status'].find(
        filter=filter,
        projection=project
    )
    data = list(result)

    return data


@app.post('/leadstage_count')
async def stage(d: List[dashboard]):

    filter = {
        'zone_name': d[0].zone_name,
        'year': d[0].year,
        'month': d[0].month
    }
    project = {
        '_id': 0,
        'zone_name': 1,
        'lead_stage': 1,
        'lead_stage_count': 1
    }

    result = client['saraswat']['da_zone_stage'].find(
        filter=filter,
        projection=project
    )
    data = list(result)

    return data


@app.post('/leadsource_count')
async def source(d: List[dashboard]):

    filter = {
        'zone_name': d[0].zone_name,
        'year': d[0].year,
        'month': d[0].month
    }
    project = {
        'lead_source': 1,
        'lead_source_count': 1,
        '_id': 0
    }

    result = client['saraswat']['da_zone_source'].find(
        filter=filter,
        projection=project
    )
    data = list(result)

    return data


@app.post('/lead_distribution')
# lead distribution bar chart
async def lead_distribution(d: List[dashboard]):

    filter = {
        'zone_name': d[0].zone_name,
        'year': d[0].year,
        'month': d[0].month
    }
    project = {
        '_id': 0,
        'user_name': 1,
        'lead_count': 1
    }

    result = client['saraswat']['da_zone_user_leadcount'].find(
        filter=filter,
        projection=project
    )
    data = list(result)

    return data


@app.post('/product_count')
# Lead Count per Product for pie chart
async def product_count(d: List[dashboard]):

    filter = {
        'zone_name': d[0].zone_name,
        'year': d[0].year,
        'month': d[0].month
    }
    project = {
        '_id': 0,
        'product_main_category_name': 1,
        'lead_count': 1
    }

    result = client['saraswat']['da_zone_product_main_category_count'].find(
        filter=filter,
        projection=project
    )
    data = list(result)

    return data


@app.post('/lead_activity')
# Lead activity count for pie chart
async def lead_activity(d: List[dashboard]):

    filter = {
        'zone_name': d[0].zone_name,
        'year': d[0].year,
        'month': d[0].month
    }
    project = {
        '_id': 0,
        'activity_type': 1,
        'lead_count': 1
    }

    result = client['saraswat']['da_zone_lead_activity_count'].find(
        filter=filter,
        projection=project
    )
    data = list(result)

    return data
