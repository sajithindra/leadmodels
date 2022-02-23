from pymongo import MongoClient
import pandas as pd
import numpy as np
from bson import ObjectId

client = MongoClient(
    'mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass%20Community&ssl=false')


################################## lead_cleaning ###############################################################


def lead_cleaning(oid):

    result = client['saraswat']['lead'].aggregate([

        {'$match': {'_id': ObjectId(oid)}},
        {
            '$addFields': {
                'id': {
                    '$toString': '$_id'
                },
                'products': '$products.id',
                'emails': '$emails.email',
                'phones': '$phones.phone',
                'projects': '$projects.id'
            }
        }, {
            '$addFields': {
                'emails': {
                    '$arrayElemAt': [
                        '$emails', 0
                    ]
                },
                'products': {
                    '$arrayElemAt': [
                        '$products', 0
                    ]
                },
                'phones': {
                    '$arrayElemAt': [
                        '$phones', 0
                    ]
                },
                'projects': {
                    '$arrayElemAt': [
                        '$projects', 0
                    ]
                }
            }
        }, {
            '$project': {
                '_id': 0,
                'leadsource_subcategory': 0,
                'remarks_further_conversation_should_be_date_wise': 0,
                'alternate_no': 0,
                'reason': 0,
                'follow_up_pending_at_below_stage': 0,
                '0': 0,
                'amount_in_lakhs': 0,
                'main_id': 0,
                'purgtwo': 0,
                'remarks_further_conversation_should_be_date_wise__if_loan_is_disbursed_pls_mention_product_&_loan_account_no': 0,
                'lock_status': 0,
                'f': 0,
                'purgone': 0,
                'lead_sent_by': 0,
                'remarks_further_conversation_should_be_date_wise__if_loan_is_disbursed_pls_mention_product_loan_account_no': 0,
                'mo_leadsource_subcategory': 0,
                'amount': 0,
                'assigned_timestamp': 0,
                'duplicate_status': 0,
                'type_of_loan': 0,
                'remarks_1st_conversation': 0,
                'remarks_further_conversation_should_be_date_wise_if_loan_is_disbursed_pls_mention_product_loan_account_no': 0,
                'data_transfer': 0,
                'transfer_note': 0,
                'name_of_the_customer': 0,
                'contact_person': 0,
                'srno': 0,
                'date_on_lead_sent': 0,
                'real_purg': 0,
                'stage': 0,
                'delete_lead_download_status': 0,
                'projects': 0,
                'assigned_to_bdo': 0,
                'timestamp': 0,
                'order': 0,
                'download_status': 0,
                'updated_at': 0,
                'created_at': 0,
                'sync_status': 0,
                'assigned_timestamp': 0,
                'assigned_date': 0,
                'alternate_mobile_no': 0,
                'lead_delay_flag': 0,
                'lead_failed_reason': 0,
                'lead_source_subcategory': 0,
                'attended_lead': 0,
                'upload_timestamp': 0,
                'residential_status': 0,
                'datetime': 0,
                'temp_id': 0,
                'leadsourcename': 0,
                'created_from': 0
            }
        }
    ])

    lead = pd.DataFrame(list(result))

    # list of columns
    column_list = list(lead.columns)

    #  converting empty strings to nan
    if 'profession' in column_list:
        lead.loc[(lead['profession'] == ''), 'profession'] = np.nan
        lead.loc[(lead['profession'] == ' '), 'profession'] = np.nan
        lead.loc[(lead['profession'] == '.'), 'profession'] = np.nan

    if 'description' in column_list:
        lead.loc[(lead['description'] == ''), 'description'] = np.nan
        lead.loc[(lead['description'] == ' '), 'description'] = np.nan

    if 'address' in column_list:
        lead.loc[(lead['address'] == ''), 'address'] = np.nan
    if 'project_id' in column_list:
        lead.loc[(lead['project_id'] == ''), 'project_id'] = np.nan

    if 'expected_closedate' in column_list:
        lead.loc[(lead['expected_closedate'] == ''),
                 'expected_closedate'] = np.nan

    if 'expected_target' in column_list:
        lead.loc[(lead['expected_target'] == ''), 'expected_target'] = np.nan

    if 'company_name' in column_list:
        lead.loc[(lead['company_name'] == ''), 'company_name'] = np.nan
        lead.loc[(lead['company_name'] == ' '), 'company_name'] = np.nan

    if 'lead_sub_stage' in column_list:
        lead.loc[(lead['lead_sub_stage'] == ''), 'lead_sub_stage'] = np.nan
        lead.loc[(lead['lead_sub_stage'] == '-1'), 'lead_sub_stage'] = np.nan

    if 'enquiry_id' in column_list:
        lead.loc[(lead['enquiry_id'] == ''), 'enquiry_id'] = np.nan

    if 'feedback' in column_list:
        lead.loc[(lead['feedback'] == ''), 'feedback'] = np.nan
        lead.loc[(lead['feedback'] == ' '), 'feedback'] = np.nan

    if 'expected_sales' in column_list:
        lead.loc[(lead['expected_sales'] == ''), 'expected_sales'] = np.nan

    if 'products' in column_list:
        lead.loc[(lead['products'] == ''), 'products'] = np.nan

    if 'product_id' in column_list:
        lead.loc[(lead['product_id'] == ''), 'product_id'] = np.nan

    if 'close_date' in column_list:
        lead.loc[(lead['close_date'] == ''), 'close_date'] = np.nan

    if 'failed_reason' in column_list:
        lead.loc[(lead['failed_reason'] == ''), 'failed_reason'] = np.nan

    if 'leadreassigned_to' in column_list:
        lead.loc[(lead['leadreassigned_to'] == ''),
                 'leadreassigned_to'] = np.nan

    if 'pin_code' in column_list:
        lead.loc[(lead['pin_code'] == ''), 'pin_code'] = np.nan

    if 'cbs_closed_date' in column_list:
        lead.loc[(lead['cbs_closed_date'] == ''), 'cbs_closed_date'] = np.nan

    if 'branch_name' in column_list:
        lead.loc[(lead['branch_name'] == ''), 'branch_name'] = np.nan

    # extracting only id from lead_id
    if 'lead_id' in column_list:
        lead.loc[:, 'lead_id'] = "".join(item for item in str(
            lead['lead_id']) if item.isdigit()).strip()
        lead.loc[(lead['lead_id'] == ''), 'lead_id'] = np.nan

    # converting dtype from Object to float
    if 'expected_sales' in column_list:
        lead['expected_sales'] = lead['expected_sales'].astype(float)
    if 'expected_target' in column_list:
        lead['expected_target'] = lead['expected_target'].astype(float)

    if 'zbdc_name' in column_list:
        # removing NoneType from columns
        lead.zbdc_name.fillna(value=np.nan, inplace=True)

    # purged_lead

    if 'purged_lead' in column_list:
        # replacing NaN with 0 in purged_lead
        lead['purged_lead'] = lead['purged_lead'].fillna(0)

        # converting purged_lead dtype to int
        lead['purged_lead'] = lead['purged_lead'].astype("int")
    else:
        lead['purged_lead'] = 0

    if 'lead_stage' in column_list and lead['lead_stage'].isnull().all() == False:

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

        lead = lead.merge(lead_stage, on='lead_stage', how='inner')
        lead = lead.drop('lead_stage', axis=1)
        lead.rename(columns={'stage': 'lead_stage'}, inplace=True)

    # lead_source

    if 'lead_source' in column_list and lead['lead_source'].isnull().all() == False:

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

        lead = lead.merge(source, on='lead_source', how='inner')

        lead = lead.drop('lead_source', axis=1)
        lead.rename(columns={'name': 'lead_source'}, inplace=True)

    # replacing empty strings
    lead.loc[(lead['lead_source'] == ''), 'lead_source'] = np.nan

   # replacing top500 with nan
    lead.loc[lead['lead_source'].str.contains(
        "Top500", na=False), 'lead_source'] = np.nan

    if 'lead_sub_stage' in column_list and lead['lead_sub_stage'].isnull().all() == False:

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
        lead = lead.merge(lstage, on='lead_sub_stage', how='inner')

        lead = lead.drop('lead_sub_stage', axis=1)
        lead.rename(columns={'name': 'lead_sub_stage'}, inplace=True)

        lead.loc[(lead['lead_sub_stage'] == ''), 'lead_sub_stage'] = np.nan

    if 'failed_reason' in column_list and lead['failed_reason'].isnull().all() == False:
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
        lead.loc[(lead['failed_reason'] == ''), 'failed_reason'] = np.nan

    # Adding a user_name column

    if 'user_id' in column_list and lead['user_id'].isnull().all() == False:

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
                    "status": 1,
                    "zone_id": 1
                }}
             ])

        # converting to dataframe
        user = pd.DataFrame(list(result))
        user.rename(columns={'phone': 'user_phone', 'email': 'user_email',
                             'status': 'user_status', 'name': 'user_name', 'zone_id': 'user_zone_id'}, inplace=True)

        lead = lead.merge(user, on="user_id", how="inner")

    # Adding a team_name column
    if 'team' in list(lead.columns) and lead['team'].isnull().all() == False:
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
        lead = lead.merge(team, on="team", how="inner")

    # Adding a role_name column

    if 'role_id' in list(lead.columns) and lead['role_id'].isnull().all() == False:
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

        lead = lead.merge(roles, on="role_id", how="inner")

    if 'createdby' in column_list and lead['createdby'].isnull().all() == False:
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
        lead = lead.merge(user, on="createdby", how="inner")
        lead = lead.drop('createdby', axis=1)
        lead.rename(columns={'created_name': 'createdby'}, inplace=True)

        # creating manager_name
    if 'manager_id' in column_list and lead['manager_id'].isnull().all() == False:

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
        lead = lead.merge(user, on="manager_id", how="inner")

    # Adding a zone_name column
    if 'user_zone_id' in list(lead.columns) and lead['user_zone_id'].isnull().all() == False:

        # fetching  zone data
        result = client['saraswat']['zone'].aggregate(
            [{
                "$addFields":
                {
                    "user_zone_id": {"$toString": "$_id"}
                }},
                {
                "$project":
                {
                    "_id": 0,
                    "user_zone_id": 1,
                    "name": 1
                }}
             ])

        # converting to dataframe
        zone = pd.DataFrame(list(result))

        zone.rename(columns={'name': 'zone_name'}, inplace=True)

        lead = lead.merge(zone, on="user_zone_id", how="inner")

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
                "product_name": 1,
                "product_category_id": 1,
                "product_main_category_id": 1,
                "status": 1

            }}
         ])

    # converting to dataframe
    product = pd.DataFrame(list(result))

    project = product[['product_name', 'products']]

    project.rename(columns={'product_name': 'project_name',
                            'products': 'project_id'}, inplace=True)

    if 'project_id' in column_list and lead['project_id'].isnull().all() == False:
        lead = lead.merge(project, on="project_id", how="inner")
        lead.loc[(lead['project_name'] == ''), 'project_name'] = np.nan

    if 'products' in column_list and lead['products'].isnull().all() == False:
        product.rename(columns={'status': 'product_status'}, inplace=True)
        lead = lead.merge(product, on="products", how="inner")
        lead.loc[(lead['product_name'] == ''), 'product_name'] = np.nan
        lead.loc[(lead['product_category_id'] == ''),
                 'product_category_id'] = np.nan
        lead.loc[(lead['product_main_category_id'] == ''),
                 'product_main_category_id'] = np.nan

    if 'product_category_id' in list(lead.columns) and lead['product_category_id'].isnull().all() == False:
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
        lead = lead.merge(product_category,
                          on="product_category_id", how="inner")
        lead.loc[(lead['product_category_name'] == ''),
                 'product_category_name'] = np.nan

    if 'product_main_category_id' in list(lead.columns) and lead['product_main_category_id'].isnull().all() == False:
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
                          on="product_main_category_id", how="inner")
        lead.loc[(lead['product_main_category_name'] == ''),
                 'product_main_category_name'] = np.nan

    # Adding year and month column
    lead['year'] = pd.DatetimeIndex(lead['created']).year
    lead['month'] = pd.DatetimeIndex(lead['created']).month

    # inserting to collection
    lead_dict = lead.to_dict('records')[0]
    client.saraswat.da_lead.update_one(
        {"_id": ObjectId(oid)}, {"$set": lead_dict}, upsert=True)


################################## achieved_cleaning ###############################################################


def achieved_cleaning(oid):

    filter = {'_id': ObjectId(oid)}
    project = {
        '_id': 0,
        'user_id': 1,
        'lead_id': 1,
        'feedback': 1,
        'product_id': 1,
        'achieved': 1,
        'year': 1,
        'month': 1,
        'status': 1,
        'manager_id': 1,
        'category_id': 1
    }

    result = client['saraswat']['achieved'].find(
        filter=filter,
        projection=project
    )

    # Converting achieved data to pandas dataframe
    df = pd.DataFrame(list(result))

    # list of columns
    column_list = list(df.columns)

    d = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
         'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12}

    df.month = df.month.map(d)

    if 'user_id' in column_list and df['user_id'].isnull().all() == False:
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
        df = df.merge(user, on="user_id", how="inner")

    if 'manager_id' in column_list and df['manager_id'].isnull().all() == False:

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
        df = df.merge(user, on="manager_id", how="inner")

    # Adding a zone_name column
    if 'zone_id' in list(df.columns) and df['zone_id'].isnull().all() == False:
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

        df = df.merge(zone, on="zone_id", how="inner")

        df = df.drop('zone_id', axis=1)

        # Adding a product_name column

    if 'product_id' in column_list and df['product_id'].isnull().all() == False:

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

        df = df.merge(product, on="product_id", how="inner")

        # removing empty strings if any
        df.loc[(df['product_category_id'] == ''),
               'product_category_id'] = np.nan
        df.loc[(df['product_main_category_id'] == ' '),
               'product_main_category_id'] = np.nan

    if 'product_category_id' in list(df.columns) and df['product_category_id'].isnull().all() == False:
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
        df = df.merge(product_category, on="product_category_id", how="inner")
        df.loc[(df['product_category_name'] == ''),
               'product_category_name'] = np.nan

    if 'product_main_category_id' in list(df.columns) and df['product_main_category_id'].isnull().all() == False:
        # fetching product_main_category data
        result = client['saraswat']['product_main_category'].aggregate(
            [

                {
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
                      on="product_main_category_id", how="inner")
        df.loc[(df['product_main_category_name'] == ''),
               'product_main_category_name'] = np.nan

    # setting achieved against CASA and CREDIT_CARD to 1
    df.loc[df['product_main_category_name'] == 'CASA', 'achieved'] = 1
    df.loc[df['product_main_category_name'] == 'CREDIT_CARD', 'achieved'] = 1

    # inserting into collection
    df_dict = df.to_dict('records')[0]
    client.saraswat.da_achieved.insert_one(df_dict)

################################## target_cleaning ###############################################################


def target_cleaning(oid):

    filter = {'_id': ObjectId(oid)}
    project = {
        '_id': 0,
        'created': 0,
        'updated_at': 0,
        'organization_id': 0
    }

    result = client['saraswat']['target'].find(
        filter=filter,
        projection=project
    )

    # Converting achieved data to pandas dataframe
    df = pd.DataFrame(list(result))

    d = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
         'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12}

    df.month = df.month.map(d)

    if 'user_id' in list(df.columns) and df['user_id'].isnull().all() == False:
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
                    "zone_id": 1

                }}
             ])

        # converting to dataframe
        user = pd.DataFrame(list(result))
        user.rename(columns={'name': 'user_name',
                             'zone_id': 'user_zone_id'}, inplace=True)

        df = df.merge(user, on="user_id", how="inner")

    # Adding a zone_name column
    if 'user_zone_id'in list(df.columns) and df['user_zone_id'].isnull().all() == False:
        # fetching  zone data
        result = client['saraswat']['zone'].aggregate(
            [{
                "$addFields":
                {
                    "user_zone_id": {"$toString": "$_id"}
                }},
                {
                "$project":
                {
                    "_id": 0,
                    "user_zone_id": 1,
                    "name": 1
                }}
             ])

        # converting to dataframe
        zone = pd.DataFrame(list(result))

        zone.rename(columns={'name': 'zone_name'}, inplace=True)

        df = df.merge(zone, on="user_zone_id", how="inner")

    l1 = list(df.columns)

    def target(df, name):

        cols = [x for x in l1 if x not in [
            'ADVANCE', 'CASA', 'TPP', 'CREDIT_CARD', 'LTSD']]
        cols.append(name)
        name_target = df[cols]

        name_target.loc[:, 'product_main_category_name'] = name
        name_target.rename(columns={name: 'target'}, inplace=True)
        name_target.dropna(subset=['target'], inplace=True)

        # Adding collecdtion to Mongodb
        name_target.reset_index(drop=True, inplace=True)
        df_dict = name_target.to_dict('records')[0]
        client.saraswat.da_target.insert_one(df_dict)

    target(df, 'CASA')
    target(df, 'ADVANCE')
    target(df, 'CREDIT_CARD')
    target(df, 'TPP')

    # ltsd
    cols = [x for x in l1 if x not in [
        'ADVANCE', 'CASA', 'TPP', 'CREDIT_CARD', 'LTSD']]
    cols.append('LTSD')
    ltsd_target = df[cols]

    ltsd_target.loc[:, 'product_main_category_name'] = 'LTSD/TD'
    ltsd_target.rename(columns={'LTSD': 'target'}, inplace=True)
    ltsd_target.dropna(subset=['target'], inplace=True)
    ltsd_target.reset_index(drop=True, inplace=True)

    df_dict = ltsd_target.to_dict('records')[0]
    client.saraswat.da_target.insert_one(df_dict)


################################## user_cleaning ###############################################################


def user_cleaning(oid):

    result = client['saraswat']['users'].aggregate(
        [
            {'$match': {'_id': ObjectId(oid)}},
            {
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
                    "phone": 1,
                    "manager_id": 1,
                    "email": 1,
                    "team": 1,
                    "organization_code": 1,
                    "organization": 1,
                    "sub_domain": 1,
                    "role_id": 1,
                    "zone_id": 1,
                    "status": 1,
                }}
        ])

    # Converting lead data to pandas dataframe
    user = pd.DataFrame(list(result))

    # list of columns
    column_list = list(user.columns)

    # converting empty strings to nan
    cat_columns = list(user.select_dtypes(['O']).columns)
    for el in cat_columns:
        user.loc[(user[el] == ''), el] = np.nan

    # Adding a zone_name column
    if 'zone_id' in column_list and user['zone_id'].isnull().all() == False:
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

        user = user.merge(zone, on="zone_id", how="inner")

        user.loc[(user['zone_name'] == ''), 'zone_name'] = np.nan

    # Adding a role_name column
    if 'role_id' in column_list and user['role_id'].isnull().all() == False:
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
        user = user.merge(roles, on="role_id", how="inner")

        user.loc[(user['role_name'] == ''), 'role_name'] = np.nan

    # Adding a team_name column
    if 'team' in column_list and user['team'].isnull().all() == False:
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
        user = user.merge(team, on="team", how="inner")

        user.loc[~(user['team'].str.len() == 24), 'team_name'] = user['team']

        user.loc[(user['team_name'] == ''), 'team_name'] = np.nan

    # creating manager_name
    if 'manager_id' in column_list and user['manager_id'].isnull().all() == False:
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
        user = user.merge(user2, on="manager_id", how="inner")
        user.loc[(user['manager_name'] == ''), 'manager_name'] = np.nan

    # Adding collection to Mongodb

    userdict = user.to_dict('records')[0]
    client.saraswat.da_user.insert_one(userdict)
