import requests
import pymysql
import sqlalchemy
import pandas as pd
import json
import time
from datetime import datetime, date, timedelta
from pytz import timezone
  
def lambda_handler(event, context):
    # Define cities
    qcities = [    # 10 German cities by population
    'Q64',    # Berlin: federal state, capital and largest city of Germany
    #'Q1055',  # Hamburg: city and federal state in the North of Germany
    #'Q1726',  # München: capital and most populous city of Bavaria, Germany
    'Q365',   # Köln: city in North Rhine-Westphalia, Germany
    #'Q1794',  # Frankfurt am Main: city in Hesse, Germany
    #'Q1022',  # Stuttgart: capital city of German federated state Baden-Württemberg
    'Q1718',  # Düsseldorf: capital city of the German federated state of North Rhine-Westphalia
    #'Q1295',  # Dortmund: city in North Rhine-Westphalia, Germany
    #'Q2066',  # Essen: city in North Rhine-Westphalia, Germany
    #'Q2079',  # Leipzig: most populous city in the German state of Saxony
                  
              # 5 Austrian cities by population
    #'Q1741',  # Wien: capital of and state in Austria
    #'Q13298', # Graz: capital of Styria, Austria
    #'Q41329', # Linz: capital city of Upper Austria, Austria
    #'Q34713', # Salzburg: capital city of the federal state of Salzburg in Austria
    #'Q1735'   # Innsbruck: capital of the state of Tyrol, Austria
         ]
    
    # Get cities:
    def get_cities(qcities):
        
        # Connect to database: (confidential)
        
        
        # Make cities_df:
        cities_list = []
        for qcity in qcities:
            url = f"https://wft-geo-db.p.rapidapi.com/v1/geo/cities/{qcity}"
            headers = {"X-RapidAPI-Host": "wft-geo-db.p.rapidapi.com", "X-RapidAPI-Key": "5f36e78ad9msh63fb19ca6dd8eeap143c53jsn68fbaa53c511"}
            response = requests.request("GET", url, headers=headers)
            time.sleep(2)
            city_df = pd.json_normalize(response.json())
            cities_list.append(city_df)
        cities_df = pd.concat(cities_list, ignore_index = True)
        cities_df = cities_df[["data.wikiDataId",
                         "data.city",
                         "data.country",
                         "data.elevationMeters",
                         "data.latitude",
                         "data.longitude",
                         "data.population"]]
        cities_df.rename(columns = {'data.wikiDataId': 'city_id',
                            'data.city': 'city' ,
                            'data.country': 'country',
                            'data.elevationMeters': 'elevation',
                            'data.latitude': 'city_latitude',
                            'data.longitude': 'city_longitude',
                            'data.population': 'population'},
                        inplace = True)
        
        # Push to MySQL:
        cities_df.to_sql('cities', if_exists='append', con=con, index=False)
        
    get_cities(qcities)
    
##########################################################################################

def get_weather(cities):
    df_list = []
    for city in cities:
        url = f"http://api.openweathermap.org/data/2.5/forecast?q={city}&appid=ae4f0da7da6fba066d93d0563204853c&units=metric"
        weather = requests.get(url)
        weather_df = pd.json_normalize(weather.json()["list"])
        weather_df["city"] = city
        df_list.append(weather_df)
    weather_combined = pd.concat(df_list, ignore_index = True)
    weather_combined = weather_combined[["pop", "dt_txt", "main.temp", "main.humidity", "clouds.all", "wind.speed", "wind.gust", "city"]]
    weather_combined.rename(columns = {'pop': 'precip_prob', 
                           'dt_txt': 'datetime', 
                           'main.temp': 'temperature', 
                           'main.humidity': 'humidity', 
                           'clouds.all': 'cloudiness', 
                           'wind.speed': 'wind_speed', 
                           'wind.gust': 'wind_gust'}, 
                inplace = True)
    weather_combined['datetime'] = pd.to_datetime(weather_combined['datetime'])
    return weather_combined

def lambda_handler(event, context):
    # Connect to database: (confidential)
    
    # Make new df for city_id and city columns from SQL query so that we eventually have the city_id as a foreign key in the weather df:
    city_df = pd.read_sql('SELECT city_id, city FROM cities', con=con)
    
    # Use the queried SQL df to have the city names as a later input for the get_weather function:
    cities = city_df['city'].to_list()
    
    # Calling the get_weather function:
    weather_output = get_weather(cities)
    
    # Merging the queried SQL df on 'city' onto the weather_output df which results in an additional column with 'city_id':
    weather_d = weather_output.merge(city_df, how = 'left')
    
    # Push to MySQL:
    weather_d.to_sql('weather', if_exists='append', con=con, index=False)

##########################################################################################

def get_airports(lat, lon):
    airport_list = []
    for i in range(len(lat)):
        url = f"https://aerodatabox.p.rapidapi.com/airports/search/location/{lat[i]}/{lon[i]}/km/50/10"
        querystring = {"withFlightInfoOnly":"true"}
        headers = {"X-RapidAPI-Host": "aerodatabox.p.rapidapi.com", "X-RapidAPI-Key": "3f65108735msh8233c12e13e197fp1b169djsn78a1d0ae07bb"}
        response = requests.request("GET", url, headers=headers, params=querystring)
        airport_df = pd.json_normalize(response.json()["items"])
        airport_list.append(airport_df)
    airports_df = pd.concat(airport_list, ignore_index = True)
    airports_df.drop_duplicates(subset ='icao', inplace = True)
    airports_df = airports_df[~airports_df.name.str.contains("Air Base", case = False)]
    airports_df.drop(columns = ['iata', 'shortName'], inplace = True)
    airports_df.reset_index(drop = True, inplace = True)
    airports_df['city_id'] = [
                          'Q64',
                          'Q64',  
                          #'Q1055',
                          #'Q1726',
                          'Q365', 
                          'Q1718',
                          #'Q1794',
                          #'Q1022',
                          #'Q1295',
                          #'Q2079',   
                          #'Q1741',
                          #'Q13298',
                          #'Q41329',
                          #'Q34713',
                          #'Q1735'
                          ]
    airports_df.rename(columns = {'name': 'airport_name',
                                'municipalityName': 'municipality_name',
                                'countryCode': 'country_code',
                                'location.lat': 'airport_latitude',
                                'location.lon': 'airport_longitude',
                                },
                                inplace = True)
    return airports_df
    
def lambda_handler(event, context):
    # Connect to database: (confidential)
    
    
    # Make new df for city_id and city columns from SQL query so that we eventually have the city_id as a foreign key in the weather df:
    city_df = pd.read_sql('SELECT city_latitude, city_longitude FROM cities', con=con)
    
    # Calling the get_airports function:
    airports_output = get_airports(city_df['city_latitude'].to_list(), city_df['city_longitude'])
    
    # Push to MySQL:
    airports_output.to_sql('airports', if_exists='append', con=con, index=False)

##########################################################################################

def get_arrivals(icao):
  arrival_list = []
  today = datetime.now().astimezone(timezone('Europe/Berlin')).date()
  tomorrow = (today + timedelta(days = 1))
  for code in icao:
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{code}/{tomorrow}T10:00/{tomorrow}T22:00"
    querystring = {"withLeg":"false","direction":"Arrival","withCancelled":"false","withCodeshared":"true",
                    "withCargo":"false","withPrivate":"false","withLocation":"false"}
    headers = {"X-RapidAPI-Key": "3f65108735msh8233c12e13e197fp1b169djsn78a1d0ae07bb", "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"}
    response = requests.request("GET", url, headers = headers, params = querystring)
    arrival_df = pd.json_normalize(response.json()["arrivals"])
    arrival_df["arrival_icao"] = code
    arrival_list.append(arrival_df)
  arrivals_df = pd.concat(arrival_list, ignore_index = True)
  arrivals_df.drop(columns = ['isCargo',
                                 'status',
                                 'callSign',
                                 'codeshareStatus',
                                 'movement.airport.iata',
                                 #'movement.actualTimeLocal',
                                 'movement.quality',
                                 'aircraft.reg',
                                 'aircraft.modeS',
                                 'movement.terminal',
                                 'movement.scheduledTimeUtc',
                                 #'movement.actualTimeUtc',
                                 #'movement.baggageBelt',
                                 #'movement.gate'
                                 ],
                      inplace = True)
  arrivals_df.rename(columns = {'number': 'flight_number',
                                   'movement.airport.icao': 'departure_icao',
                                   'movement.airport.name': 'departure_airport',
                                   'movement.scheduledTimeLocal': 'scheduled_time',
                                   'aircraft.model': 'aircraft_model',
                                   'airline.name': 'airline_name'
      
  },
                        inplace = True)
  arrivals_df['scheduled_time'] = pd.to_datetime(arrivals_df['scheduled_time'])
  return arrivals_df

def lambda_handler(event, context):
    # Connect to database: (confidential)
    
    # Make new df for city_id and city columns from SQL query so that we eventually have the city_id as a foreign key in the weather df:
    icao_df = pd.read_sql('SELECT icao FROM airports', con=con)
    
    icao = icao_df['icao'].to_list()
    
    # Call the get_arrivals:
    arrivals_output = get_arrivals(icao)
    
     # Push to MySQL:
    arrivals_output.to_sql('arrivals', if_exists='append', con=con, index=False)