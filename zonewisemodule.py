import pickle


def sales_forecast_year_num(zone_name, year):

    n_periods = year - 2021

    if zone_name == 'Zone I':

        file = open("./notebook/arima_zone1_num_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif zone_name == 'Zone II':

        file = open("./notebook/arima_zone2_num_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif zone_name == 'Zone III':

        file = open("./notebook/arima_zone3_num_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    elif zone_name == 'Zone IV':

        file = open("./notebook/arima_zone4_num_year.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])


def sales_forecast_month_num(zone_name, year, month):

    n_year = year - 2021

    # num of months from dec 2021 till the input month
    n_periods = (n_year * month)+1

    if zone_name == 'Zone I':

        file = open("./notebook/arima_zone1_num_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    if zone_name == 'Zone II':

        file = open("./notebook/arima_zone2_num_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    if zone_name == 'Zone III':

        file = open("./notebook/arima_zone3_num_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])

    if zone_name == 'Zone IV':

        file = open("./notebook/arima_zone4_num_month.pkl", "rb")
        model = pickle.load(file)

        pred = model.predict(n_periods=n_periods)

        return round(pred[-1])
