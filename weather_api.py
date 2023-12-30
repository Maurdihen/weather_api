import json
import pandas as pd
import re
import numpy as np
import os
import requests
import wget
import gzip
import io


class CitiesInformation:
    """
    Класс, определенный для загрузки идентификатора, страны и местоположения всех городов, включенных в базу данных Weather API. Этот список
    включен в файл city.list.json.gz, размещенный на сервере. Файл загружается и распаковывается для получения информации
    """
    def __init__(self):
        # Проверяем, включен ли список файлов городов с сервера в папку ./city_list
        try:
            if not os.path.isfile('./city_list/city.list.json'):
                print('Загружается файл с данными о городах, пожалуйста, подождите...')
                os.makedirs('./city_list', exist_ok=True)
                wget.download('http://bulk.openweathermap.org/sample/city.list.json.gz', './city_list/city.list.json.gz')
                fout = open('./city_list/city.list.json', 'w')
                with gzip.open('./city_list/city.list.json.gz', 'rb') as input_file:
                    with io.TextIOWrapper(input_file, encoding='utf-8') as dec:
                        print(dec.read())
                        fout.write(dec.read())
                fout.close()
                print('\t**Процесс завершен.')

            # Открытие файла cities для анализа
            with open('./city_list/city.list.json') as data_file:
                cities_info_json = json.load(data_file)

            self.cities = pd.DataFrame(cities_info_json)
            # print(self.cities)
            self.cities.columns = ['id', 'name', 'state', 'country', 'coord']
            print("Были загружены название городов и местоположение для {} мест".format(len(self.cities)))
        except ValueError as e:
            print("Не удалось загрузить файл cities..., ошибка: ", e)

    def __str__(self):
        return 'Общее количество городов в API: {}'.format(len(self.cities))

    def country_list(self):
        """
        :return: Он возвращает список всех идентификаторов стран, включенных в исходный файл
        """
        return self.cities['country'].sort_values().unique()

    def select_city_information(self, label_name, country=''):
        """
        :param label_name: label_name: любая метка, используемая для идентификации города. Заглавные буквы игнорируются
        :param country: Две заглавные буквы определяют в базе данных страну для идентификации. Функция country_list
        может использоваться для получения всех возможных значений
        :return: фрейм данных со списком всех городов, которые удовлетворяют входной метке, используемой в качестве входных данных
        """
        r = re.compile(label_name, re.IGNORECASE)
        regmatch = np.vectorize(lambda x: bool(r.match(x)))
        temp_cities = self.cities[regmatch(self.cities['name'])]
        if country == '':
            return temp_cities
        else:
            return temp_cities[temp_cities['country'] == country]


class OpenWeatherApi:
    """
    Класс, определенный для загрузки всех данных, доступных в бесплатной учетной записи.
    Список запросов содержит все запросы, доступные для каждой категории. Рекомендуется обновить эти значения в случае
предстоящей актуализации на сервере API поверх них.
    Параметры данных соответствуют структуре:
        'Имя запроса': ["Корень запроса, где {PARAM1} является первым параметром", "добавление строки, если включено больше параметра"]
        ВАЖНО: Каждая метка параметра ДОЛЖНА быть заключена в фигурные скобки для идентификации параметра
    """

    def query_list(self, query_id):
        LIST_CURRENT_WEATHER = {'by_city_name':     ["api.openweathermap.org/data/2.5/weather?q={PARAM1}", ",{PARAM2}"],
                                'by_city_ID':       ["api.openweathermap.org/data/2.5/weather?id={PARAM1}"],
                                'by_geog_coord':    ["api.openweathermap.org/data/2.5/weather?lat={PARAM1}&lon={PARAM2}"],
                                'by_zip_code':      ["api.openweathermap.org/data/2.5/weather?zip={PARAM1}", ",{PARAM2}"],
                                'by_circle':        ["http://api.openweathermap.org/data/2.5/find?lat={PARAM1}&lon={PARAM2}&cnt={PARAM3}"]}

        LIST_5_DAYS_FORCASTS = {'by_city_name':     ["api.openweathermap.org/data/2.5/forecast?q={PARAM1}", ",{PARAM2}"],
                                'by_city_ID':       ["api.openweathermap.org/data/2.5/forecast?id={PARAM1}"],
                                'by_geog_coord':    ["api.openweathermap.org/data/2.5/forecast?lat={PARAM1}&lon={PARAM2}"],
                                'by_zip_code':      ["api.openweathermap.org/data/2.5/forecast?zip={PARAM1}", ",{PARAM2}"]}

        LIST_UV_INDEX = {'for_one_location':        ["api.openweathermap.org/data/2.5/uvi?lat={PARAM1}&lon={PARAM2}"],
                         'forecast_one_location':   ["api.openweathermap.org/data/2.5/uvi/forecast?lat={PARAM1}&lon={PARAM2}&cnt={PARAM3}"],
                         'hystorical_uv_location':  ["api.openweathermap.org/data/2.5/uvi/history?lat={PARAM1}&lon={PARAM2}&cnt={PARAM3}&start={PARAM4}&end={PARAM5}"]}

        if query_id == 0:
            return LIST_CURRENT_WEATHER
        elif query_id == 1:
            return LIST_5_DAYS_FORCASTS
        elif query_id == 2:
            return LIST_UV_INDEX
        else:
            return {'-1': "No valid query ID"}

    def __init__(self, api_key):
        self.api_key = api_key

    def query_preprocessing(self, query_type, query_name, parameters):
        """
        Функция, определенная для возврата желаемого запроса, обновленного списком параметров.
        :param query_type: Целочисленное значение, представляющее семейство запросов, которые должны быть выполнены.
            0: Текущие запросы о погоде
            1: Прогноз на 5 дней
            2: УФ-индекс
        :param query_name: Имя запроса, как оно было определено в LIST_QUERY, подлежащем выполнению
        :param parameters: Список с набором параметров, которые должны быть включены в запрос
        :return: строка с запросом, заменяющим метку параметра значениями, включенными в список параметров
        """
        query = ""
        query_list_values = self.query_list(query_id=query_type)
        if "-1" in query_list_values:
            print("Не был введен действительный идентификатор списка")
            return {'error': -1}
        else:
            if query_name in query_list_values:
                list_qnr = query_list_values[query_name]
                num_param = len(re.findall("PARAM\d", list_qnr[0]))
                if num_param > len(parameters):
                    return "Количество параметров: {}, требуемое запросом: {}. Обновите список параметров".format(len(parameters), num_param)
                query = list_qnr[0]

                # Нажмите, чтобы обновить список параметров для запроса
                for i in range(num_param):
                    cad = "{PARAM" + str(i + 1) + "}"
                    query = re.sub(cad, str(parameters[i]), query)

                # Нажмите, чтобы добавить необязательный набор параметров в запрос
                j = 1
                for i in range(num_param, len(parameters)):
                    if j < len(list_qnr):
                        cad = "{PARAM" + str(i + 1) + "}"
                        query = query + re.sub(cad, str(parameters[i]), list_qnr[j])
                        j += 1
                return query
            else:
                print("Не был введен допустимый запрос")
                return {'error': -1}

    def query_execution(self, query_type, query_name, parameters):
        """
         Реализована функция для выполнения запросов в базе данных API
        :param query_type: Целочисленное значение, представляющее семейство запросов, которые должны быть выполнены.
            0: Текущие запросы о погоде
            1: Прогноз на 5 дней
            2: УФ-индекс
        :param query_name: Имя запроса, как оно было определено в LIST_QUERY, подлежащем выполнению
        :param parameters: Список с набором параметров, которые должны быть включены в запрос
        :return: словарь с выводом запроса
        """
        query = self.query_preprocessing(query_type, query_name, parameters)
        if 'error' in query:
            return {'error': -1}
        else:
            url = "https://" + query + '&appid=' + self.api_key
            api_request = requests.get(url)
            if api_request.status_code != 200:
                print("Невозможно выполнить запрос. API сообщает об ошибке: {}, {}".format(api_request.json()['message'], api_request.json()['cod']))
                return {'error': -1}
            else:
                return api_request.json()


api_key = "b3b7ebe39c95c5f0a9e893f32a7d576d"
cities_info = CitiesInformation()
# print(cities_info)
# список всех доступных городов
cities_info.country_list()

# информация по названию и индетификатору страны
cities_info.select_city_information('Cheboksary', 'RU')
cities_info.select_city_information('chebok')

query_class = OpenWeatherApi(api_key)

selected_city = cities_info.select_city_information('Cheboksary', 'RU')

city_id = int(selected_city.iloc[0]['id']) if not selected_city.empty else None
lon = int(selected_city.iloc[0]['coord']["lon"]) if not selected_city.empty else None
lat = int(selected_city.iloc[0]['coord']["lat"]) if not selected_city.empty else None

print(query_class.query_execution(0, 'by_city_ID', [city_id]))

print(query_class.query_execution(1, 'by_city_name', ["Cheboksary", "RU"]))

print(query_class.query_execution(2, 'for_one_location', [lon, lat]))