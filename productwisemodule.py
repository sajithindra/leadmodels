
import pickle


def sales_forecast_year(product_name, year):

    n_periods = year - 2021

    if product_name == 'ADVANCE':

        file = open("./notebook/arima_adv_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'TPP':

        file = open("./notebook/arima_tpp_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'LTSD/TD':

        file = open("./notebook/arima_ltsd_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'CREDIT_CARD':

        file = open("./notebook/arima_cc_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'CASA':

        file = open("./notebook/arima_casa_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])


def sales_forecast_month(product_name, year, month):

    n_year = year - 2021

    # num of months from dec 2021 till the input month
    n_periods = (n_year * month)+1

    if product_name == 'ADVANCE':

        file = open("./notebook/arima_adv_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'TPP':

        file = open("./notebook/arima_tpp_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'LTSD/TD':

        file = open("./notebook/arima_ltsd_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'CREDIT_CARD':

        file = open("./notebook/arima_creditcard_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif product_name == 'CASA':

        file = open("./notebook/arima_casa_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])
