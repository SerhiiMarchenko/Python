# Python

import matplotlib
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from dateutil import rrule
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import re
# from openpyxl.workbook import Workbook
# from openpyxl import load_workbook
from fuzzywuzzy import fuzz

# Specify Parameters
province = 'ON'  # Which province to parse?
start_year = "2010"  # I want the results to go back to at least 2006 or earlier
max_pages = 5  # Number of maximum pages to parse, EC's limit is 100 rows per page, there are about 500 stations
# Store each page in a list and parse them later

endYeary = str(datetime.now().year)
endYearm = str(datetime.now().month)
endYeard = str(datetime.now().day)
period = 'StartYear=' + start_year  + '&EndYear=' +endYeary  + '&Year=' + endYeary + '&Month=' + endYearm + '&Day=' + endYeard
# print(period)

soup_frames = []
for i in range(max_pages):
    startRow = 1 + i * 100
    print('Downloading Page: ', i)
    base_url = "http://climate.weather.gc.ca/historical_data/search_historic_data_stations_e.html?"
    queryProvince = "searchType=stnProv&timeframe=1&lstProvince={}&optLimit=yearRange&".format(province)
    queryYear = period +"&selRowPerPage=100&txtCentralLatMin=0&txtCentralLatSec=0&txtCentralLongMin=0&txtCentralLongSec=0&".format(
        start_year)
    queryStartRow = "startRow={}".format(startRow)
    response = requests.get(
        base_url + queryProvince + queryYear + queryStartRow)  # Using requests to read the HTML source
    soup = BeautifulSoup(response.text, 'html.parser')  # Parse with Beautiful Soup
    soup_frames.append(soup)
    # print(base_url + queryProvince + queryYear + queryStartRow)
    # print(queryProvince)
    # print(queryYear)
    # print(queryStartRow)


#Parsing the Environment Canada page with Beautiful Soup
# Empty list to store the station data
station_data = []

for soup in soup_frames:  # For each soup
    forms = soup.findAll("form",
                         {"id": re.compile('stnRequest*')})  # We find the forms with the stnRequest* ID using regex
    for form in forms:
        try:
            # The stationID is a child of the form
            station = form.find("input", {"name": "StationID"})['value']

            # The station name is a sibling of the input element named lstProvince
            name = form.find("input", {"name": "lstProvince"}).find_next_siblings("div")[0].text

            # The intervals are listed as children in a 'select' tag named timeframe
            timeframes = form.find("select", {"name": "timeframe"}).findChildren()
            intervals = [t.text for t in timeframes]

            # We can find the min and max year of this station using the first and last child
            years = form.find("select", {"name": "Year"}).findChildren()
            min_year = years[0].text
            max_year = years[-1].text

            # # We can find the min and max month of this station using the first and last child
            # months = form.find("select", {"name": "Month"}).findChildren()
            # min_month = months[0].text
            # max_month = months[-1].text

            # Store the data in an array
            data = [station, name, intervals, min_year, max_year]
            station_data.append(data)
        except:
            pass

# Create a pandas dataframe using the collected data and give it the appropriate column names
stations_df = pd.DataFrame(station_data, columns=['station_id', 'name', 'intervals', 'year_start', 'year_end'])
stations_df.head()

#  drop any special character in the country name
stations_df['name'] = stations_df['name'].astype(str).replace('[^a-zA-Z0-9 ]', '', regex=True)
stations_df['intervals'] = stations_df['intervals'].astype(str).replace('[^a-zA-Z0-9 ]', '', regex=True)
import numpy as np
stations_df = stations_df.replace(np.nan, 'none', regex=True) # replace empty into none
# print((stations_df['Intervals']))

#filter data without Hourly
# creating a bool series from isin()
# new_df = stations_df[stations_df["Intervals"].str.contains('Hourly') == False] # like **
# new_df = stations_df.loc[(stations_df.Intervals == "Hourly")] # whole cell
stations_df = stations_df.loc[~(stations_df.intervals == "Hourly")] # this is good  - filter whole cell revers

name = '00_' + str(province) + '_StationsID.csv'
# print(name)
import os
directory_path = os.getcwd()
directory_path = (directory_path + '/../Excel/' + name ) #for linux
print(directory_path)

stations_df.to_csv(directory_path, index=False) #drop index

# print(stations_df)

import matplotlib
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from dateutil import rrule
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import re
from fuzzywuzzy import fuzz

# We'll need `fuzzywuzzy` to look up weather stations later
# !pip install fuzzywuzzy --user
def weather_data_func(ssid):  # Windsor 4715 54738
    stationID = str(ssid).strip()
    print(stationID + '_from argument')
    start_date = datetime.strptime('Jan2019', '%b%Y')  # for weather ID
    end_date = datetime.today()  # for weather ID

    month = str(datetime.now().month)
    year = str(datetime.now().year)

    def getDaylyData(stationID, year, month):
        base_url = "http://climate.weather.gc.ca/climate_data/bulk_data_e.html?"
        query_url = "format=csv&stationID={}&Year={}&Month={}&timeframe=2".format(stationID, year, month)
        api_endpoint = base_url + query_url
        # print(api_endpoint + '   here!!!!!!!!!!') # url of data
        return pd.read_csv(api_endpoint,    skiprows=0)

    frames = []
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date):
        df = getDaylyData(stationID, dt.year, dt.month)
        frames.append(df)

    city = pd.concat(frames)
    # print(city)
    city['Date/Time'] = pd.to_datetime(city['Date/Time'])
    city['Month'] = pd.to_numeric(city['Month'])
    city['Day'] = pd.to_numeric(city['Day'])
    df = city
    # city.sort_values(by=['Year', 'Month', 'Day'])
    # city.sort_values(by='Date/Time')
    # city.head()#
    # print(city)#
    ####################
    name = '01_' + str(stationID) + '_StationsID.csv'
    # print(name)
    import os
    directory_path = os.getcwd()
    directory_path = (directory_path + '/../Excel/stations/' + name)  # for linux
    print(directory_path)
    df.to_csv(directory_path, index=False)  # drop index
    ####################
    # path = r'C:\Users\Admin\PycharmProjects\StudyExcel\HistoricalWeather\analytics-climate-canada-downloader\trunk\src\Excel\stations\01_'
    # df.to_csv(path + str(stationID) + '_city.csv', index=False)  # drop index
    # return df.to_csv(path + str(stationID) + '_city.csv', index=False) #drop index
    # stations_df.to_csv(name, index=False) #drop index#
    print('done')

