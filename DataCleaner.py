import re
from idlelib import history
from os import link

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
# We have two data sets with crime incidents in Boston.
# And we need to merge these data sets. But there is one problem.
# It is different system for crime reporting.


file_name_1 = 'crime-incident-reports-july-2012---august-2015-source-legacy-system.csv'
file_name_2 = 'crimeincidentreports.csv'


class DataCleaner:

    def __init__(self):
        self.wdc = self.get_weather_data()
        self.df1 = pd.read_csv(file_name_1)
        self.df2 = pd.read_csv(file_name_2)
        self.clean()
        self.mergedFiles = self.merge_files()
        self.mergedFiles = self.mergedFiles.drop('Unnamed: 0', 1)
        self.mergedFiles = self.mergedFiles.drop('Unnamed: 0.1', 1)
        self.add_day_night_data(self.mergedFiles)
        self.add_weather_data(self.mergedFiles, self.wdc)
        self.add_distance_data(self.mergedFiles)
        self.export_to_csv()

    def export_to_csv(self):
        self.mergedFiles.to_csv('crime-boston.csv')

    def clean(self):
        self.rename_columns()
        self.remove_unused_values()
        self.change_district_data()
        self.change_datetime_data()
        self.change_shooting_data()
        self.change_location_data()
        self.change_offense_code_group()
        self.change_UCR_PART_data()
        self.create_hour_field()

    def show_data(self):
        self.df1 = pd.read_csv(file_name_1)
        print(self.df1.shape)
        print(self.df1.shape)
        self.df2 = pd.read_csv(file_name_2)
        print(self.df2.shape)

    def rename_columns(self):
        self.df1.rename(
            columns={
                "REPORTINGAREA": "REPORTING_AREA",
                "FROMDATE": "OCCURRED_ON_DATE",
                "Year": "YEAR",
                "Month": "MONTH",
                "DAY_WEEK": "DAY_OF_WEEK",
                "UCRPART": "UCR_PART",
                "Shooting": "SHOOTING",
                "MAIN_CRIMECODE": "OFFENSE_CODE_GROUP"
            },
            inplace=True
        )

    def change_district_data(self):
        self.df1['DISTRICT'] = self.df1['DISTRICT'].map({
            'B2': 1,
            'D4': 2,
            'C11': 3,
            'A1': 4,
            'B3': 5,
            'C6': 6,
            'D14': 7,
            'E13': 8,
            'E18': 9,
            'A7': 10,
            'E5': 11,
            'A15': 12
        })

    # Format for first df > %Y-%m-%d %H:%M:%S AM/PM (2012-07-08 01:30:00 PM)
    # Format for second df > %Y-%m-%d %H:%M:%S (2012-07-08 13:30:00)
    # Then i converted all values to datetime (%Y-%m-%d %H:%M:%S) and merged it.
    def change_datetime_data(self):
        self.df1['OCCURRED_ON_DATE'] = pd.to_datetime(self.df1['OCCURRED_ON_DATE']).dt.strftime('%Y-%m-%d %H:%M:%S')

    # converting UCR_PART format for 'Part three'
    def change_UCR_PART_data(self):
        if self.df1['UCR_PART'].unique() != self.df2['UCR_PART'].unique():
            self.df1['UCR_PART'] = self.df1['UCR_PART'].map({
                'Part One': 1,
                'Part Two': 2,
                'Part Three': 3,
                'Part three': 3,
                'Other': 4
            })

            self.df2['UCR_PART'] = self.df2['UCR_PART'].map({
                'Part One': 1,
                'Part Two': 2,
                'Part Three': 3,
                'Other': 4
            })

    def change_shooting_data(self):
        self.df1['SHOOTING'] = self.df1['SHOOTING'].map({
            'No': 0,
            'Yes': 1
        })

        self.df2['SHOOTING'] = self.df2['SHOOTING'].map({
            'Y': 1
        })

    # Second data sets have columns - ‘Lat’ & ‘Long’,
    # but first don't have it. So, i need select it for our new dataframe.
    # First, i deleted - ‘(‘ and ‘(‘.
    # Then, i splitted long and lat and contained it in new columns : “Long”, “Lat”

    def change_location_data(self):
        self.df1['Location'] = \
            self.df1['Location'].apply(lambda x: ' '.join(re.findall(".*?\((.*?)\)", x)))
        df1 = pd.concat((self.df1, self.df1['Location'].apply(self._create_lat)), axis=1)

    def _create_lat(self, x):
        return pd.Series([x.split(',')[0][0:],
                          x.split(',')[1][1:-1]],
                         index=["Lat", "Long"])

    def remove_unused_values(self):
        self.df1 = self.df1.drop('COMPNOS', 1)
        self.df1 = self.df1.drop('XSTREETNAME', 1)
        self.df1 = self.df1.drop('X', 1)
        self.df1 = self.df1.drop('Y', 1)
        self.df1 = self.df1.drop('Location', 1)
        self.df2 = self.df2.drop('Location', 1)

    # Our second data sets contain name for each offence code,
    # but first data sets contain other name.
    # So, i need to research description
    # and code for each values in this column.
    # Example value:
    # VAL
    # 06xx
    # 08xx
    # MedAssist
    # Value_counts for offense code, example:
    # Motor Vehicle Accident Response > 38xx
    # Larceny > 61x / 62x / 63x
    # Medical Assistance > 30xx
    # Investigate Person > 3115
    # Drug Violation > 18xx

    def change_offense_code_group(self):
        self.df1['OFFENSE_CODE_GROUP'] = self.df1['OFFENSE_CODE_GROUP'].map({
            'VAL': 'Violations',
            '06xx': 'Larceny',
            '08xx': 'Simple Assault',
            'MedAssist': 'Medical Assistance',
            'MVAcc': 'Motor Vehicle Accident Response',
            '14xx': 'Vandalism',
            'InvPer': 'Investigate Person',
            '06MV': 'Larceny From Motor Vehicle',
            '18xx': 'Drug Violation',
            '11xx': 'Fraud',
            'PropLost': 'Property Lost',
            'TOWED': 'Towed',
            '05RB': 'Residential Burglary',
            'InvProp': 'Investigate Property',
            '04xx': 'Aggravated Assault',
            '03xx': 'Robbery',
            '07xx': 'Auto Theft',
            'PersLoc': 'Missing Person Located',
            'PropFound': 'Property Found',
            'Argue': 'Verbal Disputes',
            'Arrest': 'Warrant Arrests',
            'OTHER': 'Other'
        })
        self.df1['OFFENSE_CODE_GROUP'].fillna('Other', inplace=True)

        self.df2['OFFENSE_CODE_GROUP'] = self.df2['OFFENSE_CODE_GROUP'].map({
            'Motor Vehicle Accident Response': 'Motor Vehicle Accident Response',  #
            'Larceny': 'Larceny',  #
            'Medical Assistance': 'Medical Assistance',  #
            'Investigate Person': 'Investigate Person',  #
            'Other': 'Other',  #
            'Drug Violation': 'Drug Violation',  #
            'Simple Assault': 'Simple Assault',  #
            'Vandalism': 'Vandalism',  #
            'Verbal Disputes': 'Verbal Disputes',  #
            'Towed': 'Towed',  #
            'Investigate Property': 'Investigate Property',  #
            'Larceny From Motor Vehicle': 'Larceny From Motor Vehicle',  #
            'Property Lost': 'Property Lost',  #
            'Warrant Arrests': 'Warrant Arrests',  #
            'Aggravated Assault': 'Aggravated Assault',  #
            'Violations': 'Violations',  #
            'Fraud': 'Fraud',  #
            'Residential Burglary': 'Residential Burglary',  #
            'Missing Person Located': 'Missing Person Located',  #
            'Auto Theft': 'Auto Theft',  #
            'Robbery': 'Robbery',  #
            'Property Found': 'Property Found'  #
        })

        self.df2['OFFENSE_CODE_GROUP'].fillna('Other', inplace=True)

    def create_hour_field(self):
        self.df1['HOUR'] = self.df1['OCCURRED_ON_DATE'].apply(lambda x: x.hour)

    def merge_files(self):
        return self.df1.append(self.df2, ignore_index=True)


    def add_day_night_data(self, df):
        #month/day_start/night_start
        day_night_period = [[1, 6, 18], [2, 6, 19], [3, 6, 20],
                            [4, 5, 20], [5, 5, 21], [6, 4, 21],
                            [7, 5, 21],[8, 5, 21], [9, 6, 20],
                            [10, 6, 19], [11, 6, 17], [12, 7, 17]]
        df['Day'] = 0
        i = 0
        while i < 12:
            df['Day'].loc[
                (df['MONTH'] == day_night_period[i][0]) &
                (df['HOUR'] >= day_night_period[i][1]) &
                (df['HOUR'] <= day_night_period[i][2])
                ] = 1
            i += 1

        df['Night'] = 0
        df['Night'].loc[df['Day'] == 0] = 1

    def add_weather_data(self, df_crimes, df_weather):
        df_crimes['temperature_max'] = 0
        df_crimes['temperature_min'] = 0
        df_crimes['precipitation'] = 0
        df_crimes['snow'] = 0
        self.merge_weather_data(df_crimes, df_weather, 2012, 7, 1, 12)
        self.merge_weather_data( df_crimes, df_weather, 2013, 1, 1, 12)
        self.merge_weather_data( df_crimes, df_weather, 2014, 1, 1, 12)
        self.merge_weather_data( df_crimes, df_weather, 2014, 1, 1, 7)
        df_crimes['temperatureDifference'] = df_crimes['temperatureMax'] - df_crimes['temperatureMin']


    def add_distance_data(self, df_crimes):
        ldc = Location_Data_Creator()
        ldc.calculate_distance_from_location(df_crimes, ldc.df_uni_college, 'Universities_colleges_distance_min', 'Universities_colleges_number_near' )
        ldc.calculate_distance_from_location(df_crimes, ldc.non_public_schools,'Non-Public_schools_distance_min','Non-Public_schools_number_near')
        ldc.calculate_distance_from_location(df_crimes, ldc.public_schools, 'Public_schools_distance_min',
                                             'Public_schools_number_near')

        '''
        df = pd.read_csv('weatherdata.csv')
        df_crimes['precipitation'] = 0
        df_crimes['temperature'] = 0
        df_crimes['dewpoint'] = 0
        df_crimes['humidity'] = 0
        df_crimes['wind'] = 0
        days_months_2012 = [
            [7, 31],
            [8, 31],
            [9, 30],
            [10, 31],
            [11, 30],
            [12, 31]]

        days_month = [
            [1, 31],
            [2, 28],
            [3, 31],
            [4, 30],
            [5, 31],
            [6, 30],
            [7, 31],
            [8, 31],
            [9, 30],
            [10, 31],
            [11, 30],
            [12, 31]
        ]

        day_month_2015 = [
            [1, 31],
            [2, 28],
            [3, 31],
            [4, 30],
            [5, 31],
            [6, 30],
            [7, 31],
            [8, 31]
        ]
        self.merge_weather_data(df, df_crimes, days_months_2012, 2012)
        self.merge_weather_data(df, df_crimes, days_month, 2013)
        self.merge_weather_data(df, df_crimes, days_month, 2014)
        self.merge_weather_data(df, df_crimes, day_month_2015, 2015)

    def merge_weather_data(self, df, crimes, days_months, year):
        i = 0
        while i < len(days_months):
            day_num = 1
            while day_num <= days_months[i][1]:
                # precipitation

                precipitation = df.precipitation.loc[
                    (df.year == year) & (df.month == days_months[i][0]) & (df.day == day_num)
                    ]

                crimes.precipitation.loc[
                    (crimes.YEAR == year) & (crimes.MONTH == days_months[i][0]) & (crimes.DayNumber == day_num)
                    ] = float(precipitation)

                # temperature
                temperature = df.temperature.loc[
                    (df.year == year) & (df.month == days_months[i][0]) & (df.day == day_num)
                    ]

                crimes.temperature.loc[
                    (crimes.YEAR == year) & (crimes.MONTH == days_months[i][0]) & (crimes.DayNumber == day_num)
                    ] = float(temperature)

                # dewpoint
                dewpoint = df.dewpoint.loc[
                    (df.year == year) & (df.month == days_months[i][0]) & (df.day == day_num)
                    ]

                crimes.dewpoint.loc[
                    (crimes.YEAR == year) & (crimes.MONTH == days_months[i][0]) & (crimes.DayNumber == day_num)
                    ] = float(dewpoint)

                # humidity
                humidity = df.humidity.loc[
                    (df.year == year) & (df.month == days_months[i][0]) & (df.day == day_num)
                    ]

                crimes.humidity.loc[
                    (crimes.YEAR == year) & (crimes.MONTH == days_months[i][0]) & (crimes.DayNumber == day_num)
                    ] = float(humidity)

                # wind
                wind = df.wind.loc[
                    (df.year == year) & (df.month == days_months[i][0]) & (df.day == day_num)
                    ]

                crimes.wind.loc[
                    (crimes.YEAR == year) & (crimes.MONTH == days_months[i][0]) & (crimes.DayNumber == day_num)
                    ] = float(wind)

                print(day_num)
                day_num += 1

            print(str(i) + '_________')
            i += 1
    '''

    def merge_weather_data(self, df_crime, df_weather, year, curr_month, curr_day, end_month):

        while curr_month <= end_month:
            while curr_day <= len(df_crime):
                while curr_day <= len(df_weather.day.loc[(df_weather.year == year) & (df_weather.month == curr_month)].unique()):
                    # temperatureMin
                    tmin = df_weather.temperatureMin.loc[
                        (df_weather.year == year) & (df_weather.month == curr_month) & (df_weather.day == curr_day)
                        ]

                    df_crime.temperatureMin.loc[
                        (df_crime.YEAR == year) & (df_crime.MONTH == curr_month) & (df_crime.DAY == curr_day)
                        ] = float(tmin)

                    # temperatureMin
                    tmax = df_weather.temperatureMax.loc[
                        (df_weather.year == year) & (df_weather.month == curr_month) & (df_weather.day == curr_day)
                        ]

                    df_crime.temperatureMax.loc[
                        (df_crime.YEAR == year) & (df_crime.MONTH == curr_month) & (df_crime.DAY == curr_day)
                        ] = float(tmax)

                    # precipitation
                    p = df_weather.precipitation.loc[
                        (df_weather.year == year) & (df_weather.month == curr_month) & (df_weather.day == curr_day)
                        ]

                    df_crime.precipitation.loc[
                        (df_crime.YEAR == year) & (df_crime.MONTH == curr_month) & (df_crime.DAY == curr_day)
                        ] = float(p)

                    # snow
                    sn = df_weather.snow.loc[
                        (df_weather.year == year) & (df_weather.month == curr_month) & (df_weather.day == curr_day)
                        ]

                    df_crime.snow.loc[
                        (df_crime.YEAR == year) & (df_crime.MONTH == curr_month) & (df_crime.DAY == curr_day)
                        ] = float(sn)

                    curr_day += 1

                curr_month += 1





    def get_weather_data(self):
        wdc = Weather_Data_Creator()
        wdc.df = wdc.df.drop('Unnamed: 0', 1)
        wdc.df.date = pd.to_datetime(wdc.df.date)
        wdc.df['year'] = wdc.df['date'].apply(lambda x: x.year)
        wdc.df['month'] = wdc.df['date'].apply(lambda x: x.month)
        wdc.df['day'] = wdc.df['date'].apply(lambda x: x.day)
        wdc.df.precipitation = pd.to_numeric(wdc.df.precipitation, errors='coerce')
        wdc.df.snow = pd.to_numeric(wdc.df.snow, errors='coerce')
        wdc.df = wdc.df.drop('snow_depth', 1)
        return wdc.df




class Weather_Data_Creator:

    def __init__(self):
        self.df = pd.DataFrame()
        self.df['date'] = 0
        self.df['temperatureMin'] = 0
        self.df['temperatureMax'] = 0
        self.df['precipitation'] = 0
        self.df['snow'] = 0
        self.df['snow_depth'] = 0
        wd_2012 = Weather_Data_Taker("https://www.usclimatedata.com/climate/boston/massachusetts/united-states/usma0046/2012/")
        wd_2013 = Weather_Data_Taker("https://www.usclimatedata.com/climate/boston/massachusetts/united-states/usma0046/2013/")
        wd_2014 = Weather_Data_Taker("https://www.usclimatedata.com/climate/boston/massachusetts/united-states/usma0046/2014/")
        wd_2015 = Weather_Data_Taker("https://www.usclimatedata.com/climate/boston/massachusetts/united-states/usma0046/2015/")
        wd_2012.get_weather_data()
        wd_2013.get_weather_data()
        wd_2014.get_weather_data()
        wd_2015.get_weather_data()
        self.df.to_csv('total-weather.csv')

class Weather_Data_Taker:
    def __init__(self, link):
        self.page = requests.get(link)
        self.soup = BeautifulSoup(self.page.content, 'html.parser')
        self.history = self.soup.find("div", id="history")
        self.left_column = history.find("div", class_="left_column")
        self.history_data = self.left_column.find("table", class_="daily_climate_table")

    def get_date(self, history_data):
        date = history_data.findAll(
            "td",
            class_="align_left daily_climate_table_td_day"
        )

        i = 0
        array = []

        while i < len(date):
            array += date[i]
            i += 1

        return array

    def get_temperature_min(self, history_data):

        temperature = history_data.findAll(
            "td",
            class_="align_right climate_table_data_td temperature_blue"
        )

        i = 0
        array = []

        while i < len(temperature):
            array += temperature[i]

            i += 1

        return array

    def get_temperature_max(self, history_data):
        temperature = history_data.findAll(
            "td",
            class_="align_right climate_table_data_td temperature_red "
        )

        i = 0
        array = []

        while i < len(temperature):
            array += temperature[i]

            i += 1

        return array


    def get_precip(self, history_data):

        precip = history_data.findAll(
            "td",
            class_="align_right climate_table_data_td"
        )

        i = 0
        array = []

        while i < len(precip):
            array += precip[i]
            i += 3

        return array

    def get_snow(self, history_data):
        snow = history_data.findAll(
            "td",
            class_="align_right climate_table_data_td"
        )

        i = 1
        array = []

        while i < len(snow):
            array += snow[i]
            i += 3

    def get_weather_data(self):
        i = 1
        while i <= 12:
            date_array, temperature_min_array, temperature_max_array, precip_array, snow_aray, snow_depth_array = self.parse(
                self.link + str(i)
            )
            print(i)

            k = 0

            while k < len(date_array):
                df = df.append(
                    {
                        'date': date_array[k],
                        'temperatureMin': temperature_min_array[k],
                        'temperatureMax': temperature_max_array[k],
                        'precipitation': precip_array[k],
                        'snow': snow_aray[k],
                        'snow_depth': snow_depth_array[k]
                    },
                    ignore_index=True
                )
                k += 1

            i += 1

    def get_snow_depth(self, history_data):
        snow_depth = history_data.findAll("td", class_="align_right climate_table_data_td")

        i = 2
        array = []
        while i < len(snow_depth):
            array += snow_depth[i]

            i += 3

        return array

    def create_BS(link):

        page = requests.get(link)

        # creata BeautifulSoup parser
        soup = BeautifulSoup(page.content, 'html.parser')

        # history
        history = soup.find("div", id="history")

        # left_column
        left_column = history.find("div", class_="left_column")

        history_data = left_column.find("table", class_="daily_climate_table")

        return history_data

    def parse(self, link):

        history_data = self.create_BS(link)

        # THEN

        # DATE
        date_array = self.get_date(history_data)

        # temperature min
        temperature_min_array = self.get_temperature_min(history_data)

        # temperature max
        temperature_max_array = self.get_temperature_max(history_data)

        # precip
        precip_array = self.get_precip(history_data)

        # snow
        snow_array = self.get_snow(history_data)

        # snow depth
        snow_depth_array = self.get_snow_depth(history_data)

        return date_array, temperature_min_array, temperature_max_array, precip_array, snow_array, snow_depth_array




# we need to make a decision which data we are going to use
# and which of them are not going to be worth adding

# we need to find the columns in both files in order to connect the files

# reportingarea, weapontype, shooting, ucrpart

# we will take data only in the or 2012- 2015 becoase the date since 2015
# do not have information about :
# WEAPONTYPE
# DOMESTIC
# SHIFT
# STREETNAME
# XSTREETNAME

# so that we are going to merge the file over
"""
REPTDISTRICT - DISTRICT
REPORTINGAREA - REPORTING_AREA
FROMDATE - OCCURRED_ON_DATE
Year - YEAR
Month - MONTH
DAY_WEEK - DAY_OF_WEEK
UCRPART - UCR_PART
Shooting - SHOOTING
Location - Location
MAIN_CRIMECODE - OFFENSE_CODE_GROUP
X\Y - Lat\Long
"""

#I found Boston data for diffenrent object. I selected for merging this data:
#Colleges and Universities location
#Non Public Schools location
#Public Schools location

class Location_Data_Creator:
    # ...number_near - (Number of some object, which is nearer than 3 km for offenses location)
    # ...distance_min - (Minimum distance some object near offenses location)

    def __init__(self):
        self.df_uni_college = pd.read_csv('data/Colleges_and_Universities.csv')
        self.public_schools = pd.read_csv('data/Public_Schools.csv')
        self.non_public_schools = pd.read_csv('data/df-pschools-2.csv')





    def distance(self, x1, y1, x2, y2):
        return ((x1 - x2) ** 2 + (y1 - y2) ** 2) ** (1 / 2)


    def calculate_distance_from_location(self, crimes_data, loc_data, distance_min_title, number_near_title):
        crimes_data[distance_min_title] = 0
        crimes_data[number_near_title] = 0

        i = 0
        while i < len(crimes_data):
            k = 0
            array = []
            while k < len(loc_data):
                array.append(
                    self.distance(
                        loc_data.Latitude[k],
                        loc_data.Longitude[k],
                        crimes_data.Lat[i],
                        crimes_data.Long[i]
                    )
                )

                k += 1

            n = 0
            number_near = 0

            while n < len(array):
                if (array[n] * 100000) < 3000:
                    number_near += 1
                    n += 1
                else:
                    n += 1

            a = np.array(array)

            crimes_data[distance_min_title][i] = a.min() * 100000
            crimes_data[number_near_title][i] = number_near
            i += 1



DataCleaner()




