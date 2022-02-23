
import pickle
import pandas as pd
import numpy as np
from pymongo import MongoClient
from bson import ObjectId


# mongo client
client = MongoClient(
    'mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false')


def customer_profile(id):
    filter = {
        '_id': ObjectId(id)
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
    if(data['profession'].values.all() == '') or (str(data['profession'].values.all()) == 'nan') == True:
        profile = 'profession not valid'
    else:
        profession_encoded = profession_encoder.transform(
            data[['profession']])

        prediction = model.predict(profession_encoded)

        if prediction[0] == 1:
            profile = 'high'
        else:
            profile = 'low'

    return profile


def lead_quality(id, profile):
    filter = {
        'id': id
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

    if profile == 'profession not valid':
        lead_quality = 'Not enough data'

    else:
        lead['customer_profile'] = profile

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

    return lead_quality


def high_followup_count(id, lead_quality):
    filter = {
        'id': id
    }
    project = {
        '_id': 0,
        'lead_stage': 1,

    }
    result = client['saraswat']['da_lead'].find_one(
        filter=filter,
        projection=project
    )

    lead = dict(result)

    if lead_quality == 'high' and lead['lead_stage'] == 'Follow Up':
        return 1
    else:
        return 0
