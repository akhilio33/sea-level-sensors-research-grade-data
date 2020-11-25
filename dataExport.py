import numpy as np
import pandas as pd

import dateutil.parser as date_parser
import datetime
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import pytz

import api_scraper

# collect the list of sensors with water level data
api_data_water = api_scraper.get_sensors_with_water()
sensors_water = pd.DataFrame(api_data_water)


# get water level data from a sensor; input request format should be YYYY-mm-dd HH:MM, in EST (daylight savings time)
def get_sls_water_level_data(sensor_name, start_date, end_date):
    # API stores data in GMT. Input should be in EST, so we will convert to GMT before making the API call
    start_dt = datetime.strptime(start_date, "%Y-%m-%d %H:%M")
    est = pytz.timezone("America/New_York")
    start_est = est.localize(start_dt)
    start_gmt = start_est.astimezone(pytz.timezone("GMT"))
    start_gmt_str = start_gmt.strftime("%Y-%m-%d %H:%M")

    end_dt = datetime.strptime(end_date, "%Y-%m-%d %H:%M")
    est = pytz.timezone("America/New_York")
    end_est = est.localize(end_dt)
    end_gmt = end_est.astimezone(pytz.timezone("GMT"))
    end_gmt_str = end_gmt.strftime("%Y-%m-%d %H:%M")

    # organizing the retrieved data
    link = sensors_water[sensors_water["desc"] == sensor_name]["link"].iloc[0]
    elevation = sensors_water[sensors_water["desc"] == sensor_name]["elev"].iloc[0]
    data = api_scraper.get_obs_for_link(link, start_gmt_str, end_gmt_str)
    data = pd.DataFrame(data, columns=["value", "timestamp"])

    # convert from GMT back to EST
    data['timestamp'] = data['timestamp'].map(lambda time: (time.astimezone(pytz.timezone("America/New_York"))))

    # sort data by timestamp
    data.sort_values(by=['timestamp'], inplace=True)

    # convert from meters to feet and make adjustments to elevation that correspond to updated survey data
    # fort pulaski sensors were resurveyed for benchmarking purposes; elevation fixed with a vertical offset:
    if elevation is None:
        elevation = 0

    data["adj_value"] = data["value"].map(lambda value: float(elevation) * 3.281 + float(value) * 3.281)

    # offsetting water level of sls data, needs to be converted to feet and benchmarked against same datum
    # benchmark conversion from navd88 to mllw is complicated, need to use this tool, different for each location:
    # https://vdatum.noaa.gov/vdatumweb/vdatumweb?a=164804720190625

    return data


# gather only sensors with updated elevation surveys
surveyed_sensors = pd.read_excel('./SensorInstallationDetails.xlsx')  # sensor installation doc updated OCT 2020
ss_splitNames = pd.DataFrame(
    surveyed_sensors[~np.isnan(surveyed_sensors['Survey top of box (feet)'])]['Location'].apply(
        lambda x: x.split()[0:2]))

surveyed_sensor_names_split = []
for item in ss_splitNames.Location:
    surveyed_sensor_names_split.append(item)

surveyed_sensor_names = []
for name in sensors_water.desc:
    if name.split()[0:2] in surveyed_sensor_names_split:
        surveyed_sensor_names.append(name)

# Surveyed Sensors during Dorian

# format should be YYYY-mm-dd HH:MM, in EST (daylight savings time)
dor_spatial_start = '2019-08-25 00:00'
dor_spatial_end = '2019-09-14 00:00'

# compile all sensor data together
sensor_data = []

# data metrics
filter_data_loss = []
num_returns = []
filter_data_loss_name = []

for index in range(sensors_water.index):

    #     # only take sensors that have recent surveys
    #     if sensors_water.desc[index] in surveyed_sensor_names:

    # get data for the current sensor
    water_data = get_sls_water_level_data(sensors_water.desc[index], dor_spatial_start, dor_spatial_end)

    if water_data.size > 0:
        water_data['lat'] = round(sensors_water.coords[index][1], 2)
        water_data['lng'] = round(sensors_water.coords[index][0], 2)
        water_data['desc'] = sensors_water.desc[index]
        water_data['water_level'] = water_data['adj_value']
        water_data = water_data[['desc', 'lat', 'lng', 'timestamp', 'water_level']]

        """
        nearest neighbor filtering - assuming tidal water levels won't change more than 0.3ft (adjustable), aside from waves.
        this filter acts similar to a noise filter, which was also tested, that filters out high frequency noise, generally caused by waves/wind
        assigns NaN value instead of removing data point
        """
        temp_col = water_data['water_level'].copy()
        temp_col[(abs(temp_col - temp_col.shift(1)) > 0.3) & (abs(temp_col - temp_col.shift(-1)) > 0.3)] = np.NaN
        water_data['filtered_water_level'] = temp_col

        # calculate data metrics
        count_before_filter = len(water_data)
        num_returns.append(count_before_filter)
        count_after_filter = np.isnan(water_data['filtered_water_level']).sum()
        filter_data_loss.append((count_before_filter - count_after_filter) / count_before_filter)
        filter_data_loss_name.append(sensors_water.desc[index])
        # print(count_before_filter,count_after_filter)

        sensor_data.append(water_data)

# concatenate all data together
dorian_sensor_data = pd.concat(sensor_data)

# export to CSV
dorian_sensor_data.to_csv('./dorian_sensor_data.csv')
