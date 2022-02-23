
import pickle


def sales_forecast_year(product_name, year):

    n_periods = year - 2021

    if product_name == 'TAB BANKING-I':

        file = open("./notebook/arima_tab_banking1_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'Current':

        file = open("./notebook/arima_current_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'Savings':

        file = open("./notebook/arima_savings_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'CREDIT CARD':

        file = open("./notebook/arima_CREDITCARD_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'General Insurance':

        file = open("./notebook/arima_general_insurance_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'Health Insurance':

        file = open("./notebook/arima_health_insurance_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'Life Insurance':

        file = open("./notebook/arima_life_insurance_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'Mutual Fund (Lump Sum)':

        file = open("./notebook/arima_mf_lumpsum_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'Mutual Fund (SIP)':

        file = open("./notebook/arima_mf_sip_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'Term Deposit':

        file = open("./notebook/arima_term_deposit_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'Commercial Loan':

        file = open("./notebook/arima_commercial_loan_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'Retail':

        file = open("./notebook/arima_retail_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'TABLE CHART':

        file = open("./notebook/arima_table_chart_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])


def sales_forecast_month(product_name, year, month):

    n_year = year - 2021

    # num of months from dec 2021 till the input month
    n_periods = (n_year * month)+1

    if product_name == 'TAB BANKING-I':

        file = open("./notebook/arima_tab_banking1_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'Current':

        file = open("./notebook/arima_current_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'Savings':

        file = open("./notebook/arima_savings_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'CREDIT CARD':

        file = open("./notebook/arima_credit_card_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'General Insurance':

        file = open("./notebook/arima_general_insurance_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'Health Insurance':

        file = open("./notebook/arima_health_insurance_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'Life Insurance':

        file = open("./notebook/arima_life_insurance_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'Mutual Fund (Lump Sum)':

        file = open("./notebook/arima_mf_lumpsum_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'Mutual Fund (SIP)':

        file = open("./notebook/arima_mf_sip_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'Term Deposit':

        file = open("./notebook/arima_term_deposit_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'Retail':

        file = open("./notebook/arima_retail_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'Commercial Loan':

        file = open("./notebook/arima_commercial_loan_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])
