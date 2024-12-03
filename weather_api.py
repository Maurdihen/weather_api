import json  # Importing JSON library to handle JSON data.
import pandas as pd  # Importing pandas for data manipulation and analysis.
import re  # Importing regular expression module for pattern matching.
import numpy as np  # Importing numpy for numerical operations.
import os  # Importing os module to interact with the operating system.
import requests  # Importing requests for making HTTP requests.
import wget  # Importing wget for downloading files from the internet.
import gzip  # Importing gzip for handling compressed files.
import io  # Importing io for handling streams of data.

# Initialize the CitiesInformation class, managing city list operations
class CitiesInformation:
    """
    Class for managing city information data. This includes downloading,
    processing, and providing access to a list of cities supported by the Weather API.
    """

    def __init__(self):
        # Check if the city list file exists locally; otherwise, download it
        try:
            # Verify the existence of the city list JSON file
            if not os.path.isfile('./city_list/city.list.json'):
                print('Downloading city list file, please wait...')

                # Ensure the output directory exists
                os.makedirs('./city_list', exist_ok=True)

                # Download the gzipped city list file
                # Using wget to download files; alternative methods include requests or urllib
                wget.download('http://bulk.openweathermap.org/sample/city.list.json.gz',
                              './city_list/city.list.json.gz')

                # Prepare to extract and save the decompressed JSON file
                fout = open('./city_list/city.list.json', 'w')
                with gzip.open('./city_list/city.list.json.gz', 'rb') as input_file:
                    with io.TextIOWrapper(input_file, encoding='utf-8') as dec:
                        print(dec.read())  # Read and print the decompressed data for validation
                        fout.write(dec.read())  # Write the decompressed data to a new file

                fout.close()
                print('\t**Process completed successfully.')

            # Open and load the city list JSON file into a DataFrame
            with open('./city_list/city.list.json') as data_file:
                cities_info_json = json.load(data_file)

            self.cities = pd.DataFrame(cities_info_json)
            # Rename DataFrame columns for better clarity
            self.cities.columns = ['id', 'name', 'state', 'country', 'coord']
            print("City names and locations for {} places have been loaded.".format(len(self.cities)))
        except ValueError as e:
            # Handle JSON parsing errors gracefully
            print("Failed to load city file..., error: ", e)

    # Customize string representation to show the total number of cities
    def __str__(self):
        # Return a human-readable string representation of the class
        return 'Total number of cities in the API: {}'.format(len(self.cities))

    def country_list(self):
        """
        :return: Sorted list of all unique country codes in the city list
        """
        return self.cities['country'].sort_values().unique()

    # Use regex matching for flexible and case-insensitive city search
    def select_city_information(self, label_name, country=''):
        """
        Find city information based on name and optionally a specific country code.
        :param label_name: The name (or partial name) of the city (case-insensitive).
        :param country: Optional 2-letter country code to filter results further.
        :return: DataFrame containing matching city information.
        """
        # Compile a case-insensitive regex for matching city names
        r = re.compile(label_name, re.IGNORECASE)
        regmatch = np.vectorize(lambda x: bool(r.match(x)))

        # Filter DataFrame based on the regex match
        temp_cities = self.cities[regmatch(self.cities['name'])]
        if country == '':
            return temp_cities
        else:
            # Further filter by country code, if specified
            return temp_cities[temp_cities['country'] == country]


class OpenWeatherApi:
    """
    Class to handle interactions with the OpenWeather API. It supports querying
    current weather, 5-day forecasts, and UV index data.
    """

    def query_list(self, query_id):
        """
        Return a dictionary of API query templates based on the type of query.
        :param query_id: Integer representing the type of query (0, 1, or 2).
        :return: Dictionary of query templates for the specified type.
        """
        # Define query templates for current weather data
        LIST_CURRENT_WEATHER = {
            'by_city_name': ["api.openweathermap.org/data/2.5/weather?q={PARAM1}", ",{PARAM2}"],
            'by_city_ID': ["api.openweathermap.org/data/2.5/weather?id={PARAM1}"],
            'by_geog_coord': ["api.openweathermap.org/data/2.5/weather?lat={PARAM1}&lon={PARAM2}"],
            'by_zip_code': ["api.openweathermap.org/data/2.5/weather?zip={PARAM1}", ",{PARAM2}"],
            'by_circle': ["http://api.openweathermap.org/data/2.5/find?lat={PARAM1}&lon={PARAM2}&cnt={PARAM3}"]
        }

        # Define query templates for 5-day weather forecasts
        LIST_5_DAYS_FORECASTS = {
            'by_city_name': ["api.openweathermap.org/data/2.5/forecast?q={PARAM1}", ",{PARAM2}"],
            'by_city_ID': ["api.openweathermap.org/data/2.5/forecast?id={PARAM1}"],
            'by_geog_coord': ["api.openweathermap.org/data/2.5/forecast?lat={PARAM1}&lon={PARAM2}"],
            'by_zip_code': ["api.openweathermap.org/data/2.5/forecast?zip={PARAM1}", ",{PARAM2}"]
        }

        # Define query templates for UV index data
        LIST_UV_INDEX = {
            'for_one_location': ["api.openweathermap.org/data/2.5/uvi?lat={PARAM1}&lon={PARAM2}"],
            'forecast_one_location': [
                "api.openweathermap.org/data/2.5/uvi/forecast?lat={PARAM1}&lon={PARAM2}&cnt={PARAM3}"],
            'historical_uv_location': [
                "api.openweathermap.org/data/2.5/uvi/history?lat={PARAM1}&lon={PARAM2}&start={PARAM4}&end={PARAM5}"]
        }

        # Return the appropriate query list based on the query ID
        if query_id == 0:
            return LIST_CURRENT_WEATHER
        elif query_id == 1:
            return LIST_5_DAYS_FORECASTS
        elif query_id == 2:
            return LIST_UV_INDEX
        else:
            # Return an error if the query ID is invalid
            return {'-1': "No valid query ID"}

    def __init__(self, api_key):
        # Initialize the API class with the provided API key
        self.api_key = api_key

    def query_preprocessing(self, query_type, query_name, parameters):
        """
        Prepare an API query string by filling in parameters.
        :param query_type: Type of query (0: current weather, 1: forecasts, 2: UV index).
        :param query_name: Specific query template name.
        :param parameters: List of parameters to populate the query.
        :return: Fully populated query string.
        """
        # Retrieve the query template dictionary for the specified type
        query_list_values = self.query_list(query_id=query_type)

        if "-1" in query_list_values:
            print("Invalid query type provided")
            return {'error': -1}

        # Validate if the requested query name exists
        if query_name in query_list_values:
            list_qnr = query_list_values[query_name]
            # Count required parameters from the template
            num_param = len(re.findall("PARAM\d", list_qnr[0]))

            if num_param > len(parameters):
                return f"Parameters count mismatch: required={num_param}, provided={len(parameters)}."

            # Replace parameter placeholders with actual values
            query = list_qnr[0]
            for i in range(num_param):
                query = re.sub(rf"\{{PARAM{i + 1}\}}", str(parameters[i]), query)

            # Append optional parameters, if available
            for i, optional_param in enumerate(parameters[num_param:], start=1):
                if i < len(list_qnr):
                    query += list_qnr[i].replace(f"{{PARAM{i + 1}}}", str(optional_param))

            return query
        else:
            print("Invalid query name provided")
            return {'error': -1}

    # Execute the API query and return a JSON response with weather or UV data
    def query_execution(self, query_type, query_name, parameters):
        """
        Execute the API query and return the results as JSON.
        :param query_type: Type of query (0: current weather, etc.).
        :param query_name: Specific query template name.
        :param parameters: List of parameters to populate the query.
        :return: JSON response from the API.
        """
        query = self.query_preprocessing(query_type, query_name, parameters)

        # Handle HTTP status codes to ensure proper error handling from the API
        if 'error' in query:
            return {'error': -1}

        # Construct the full API URL with the API key
        url = f"https://{query}&appid={self.api_key}"
        api_request = requests.get(url)

        # Handle API errors and print relevant messages
        if api_request.status_code != 200:
            print(f"API Error: {api_request.json()['message']} (Code {api_request.json()['cod']})")
            return {'error': -1}

        # Return the parsed JSON response
        return api_request.json()
