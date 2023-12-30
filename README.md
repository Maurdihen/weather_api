# # OpenWeatherMap - подключение к API

**weather_api_class.py ** был разработан, чтобы обеспечить простой способ загрузки погодных запросов из API [https://openweathermap.org/](https://openweathermap.org/). Он состоит из двух классов:

1. *Информация о городах*: класс, определенный для загрузки идентификатора, страны и местоположения всех городов, включенных в базу данных Weather API. Список включен в файл [city.list.json.gz](http://bulk.openweathermap.org/sample/city.list.json.gz), размещенный на сервере. Файл загружается локально и распаковывается во вложенную папку `./city_list/` с помощью одного из методов, определенных в классе.
2. *Open Weather Api*: предоставляет методы для загрузки всех запросов, доступных в бесплатной учетной записи для каждой категории.

Оба класса должны быть импортированы в ваш файл python, чтобы использовать их методы, описанные ниже.

---

## Кдасс: CitiesInformation()
Для создания экземпляра этого класса не требуется никаких параметров. Следовательно, можно определить следующую переменную:

`>>>cities_info = CitiesInformation()`

#### Методы:

1. Получите количество городов, перечисленных в базе данных:
```
print(cities_info)
>>>Общее количество городов в API: 209579
```
2. В некоторых запросах требуется указание страны. Метка каждого из них в базе данных сообщается путем вызова метода *country_list()* без параметров. Он возвращает массив с идентификатором страны.
```
>>>cities_info.country_list()
array(['', 'AD', 'AE', 'AF', 'AG', 'AI', 'AL', 'AM', 'AO', 'AQ', 'AR',
       'AS', 'AT', 'AU', 'AW', 'AX', 'AZ', 'BA', 'BB', 'BD', 'BE', 'BF',
       'BG', 'BH', 'BI', 'BJ', 'BL', 'BM', 'BN', 'BO', 'BQ', 'BR', 'BS',

       'TN', 'TO', 'TR', 'TT', 'TV', 'TW', 'TZ', 'UA', 'UG', 'US', 'UY',
       'UZ', 'VA', 'VC', 'VE', 'VG', 'VI', 'VN', 'VU', 'WF', 'WS', 'XK',
       'YE', 'YT', 'ZA', 'ZM', 'ZW'], dtype=object)
```
3. Чтобы получить идентификатор, страну и местоположение города, используя только часть его названия, предусмотрен метод *select_city_information(label_name, [страна])*. Где параметр *[страна]* является необязательным и задается выводом вышеупомянутой функции. Если он не задан, функции возвращают все города, которые соответствуют метке города. В примере указана информация обо всех городах в США, начинающаяся с метки *"сан" и "*" (без учета регистра):
```
>>>cities_info.select_city_information('san Ant', 'US')
                             city_coordinates  ...            city_name
116988   {'lat': 29.42412, 'lon': -98.493629}  ...          San Antonio
196485  {'lat': 28.336109, 'lon': -82.274529}  ...          San Antonio
206530  {'lat': 34.15556, 'lon': -117.656441}  ...  San Antonio Heights
```
В приведенном выше примере представлены города, которые соответствуют метке *"san Ant"*. Возвращается фрейм данных Pandas. Следовательно, для городов, начинающихся с *"house", можно использовать следующие параметры*:
```
cities_name = cities_info.select_city_information('houst')
cities_name[['city_country', 'city_name', 'city_id', 'city_coordinates']]
       city_country       city_name  city_id  \
3691             CA         Houston  5977783   
11910            US  Houston County  4068296   
109205           US  Houston County  4201512   
116948           US         Houston  4699066   
160841           GB         Houston  2646507   
198194           US         Houston  4391354   
198440           US         Houston  4430529   
205021           US         Houston  5194369   
208119           US         Houston  5864312   
                              city_coordinates  
3691    {'lat': 54.399761, 'lon': -126.670082}  
11910    {'lat': 31.150181, 'lon': -85.316597}  
109205   {'lat': 32.433491, 'lon': -83.649902}  
116948   {'lat': 29.763281, 'lon': -95.363274}  
160841     {'lat': 55.868591, 'lon': -4.55201}  
198194    {'lat': 37.32616, 'lon': -91.955994}  
198440   {'lat': 33.898449, 'lon': -88.999229}  
205021    {'lat': 40.24646, 'lon': -80.211449}  
208119   {'lat': 61.63028, 'lon': -149.818054}  
```
---

## Класс: OpenWeatherApi()
Этот класс предоставляет доступ к базе данных. Для выполнения любого запроса требуется действительный API-ключ, предоставленный сервером. Создание экземпляра класса выполняется с помощью следующей строки кода:
```
query_class = Openweathermap(api_key)
```
У него есть два фундаментальных метода, довольно тесно связанных друг с другом с одним и тем же списком параметров, это:

1. *query_preprocessing(query_type, query_name, parameters)*
2. *query_execution(query_type, query_name, parameters)*

Единственная разница заключается в том, что первый предоставляет только запрос, который должен быть выполнен на сервере, а второй выполняет его. То 

#### Определение параметров:
1. **query_type**: в соответствии с сервисами, предоставляемыми API для бесплатной учетной записи, можно использовать следующую группу запросов:

- API текущей погоды, (query_type = 0): Доступ к текущим данным о погоде для любого местоположения на Земле, включая более 200 000 городов, обновленным на основе глобальных моделей и данных с более чем 40 000 метеостанций, [Ссылка](https://openweathermap.org/current)

- API прогноза на 5 дней / 3 часа, (query_type = 1): прогноз на 5 дней для каждого местоположения или города. Он включает данные о погоде каждые 3 часа, [Ссылка](http://openweathermap.org/forecast 5)

- УФ-индекс, (query_type = 2): Доступ к текущим/прогнозируемым/историческим УФ-данным для более чем 200 000 городов, [Ссылка](https://openweathermap.org/api/uvi)

2. **query_name**: определяет запрос, который должен быть выполнен, на основе выбранного типа запроса. Ниже приведены параметры определенных меток запросов:

| query_type | query_name | Parameters|
|:----: | :----: | :----:|
|0| 'by_city_name' | city name, [country code]|
| | 'by_city_ID' | city_ID |
| |'by_geog_coord' | lat, lon |
| |'by_zip_code' | zip code,[country code]|
| |'by_circle' |lat, lon, cnt |
|1| 'by_city_name' | city name, [country code]|
| | 'by_city_ID' | city_ID |
| |'by_geog_coord' | lat, lon |
| |'by_zip_code' | zip code,[country code]|
| 2|'for_one_location' | lat, lon |
| |'forecast_one_location' | lat, lon, cnt|
| |'hystorical_uv_location' |lat, lon, cnt, start, end |

Имя запроса должно быть введено в виде строки между " ", а параметры - в виде списка [ ]

#### Примеры:
1. Текущая погода в Хьюстоне на основе идентификатора в базе данных (приведен выше):
```
>>>query_class.query_execution(0, 'by_city_ID', [4699066])
{'sys': {'country': 'US', 'sunrise': 1553775346, 'id': 4850, 'sunset': 1553819825, 'type': 1, 'message': 0.0092}, 'name': 'Houston', 'clouds': {'all': 1}, 'dt': 1553820748, 'wind': {'gust': 5.1, 'speed': 2.1}, 'visibility': 16093, 'base': 'stations', 'cod': 200, 'coord': {'lat': 29.76, 'lon': -95.36}, 'weather': [{'description': 'clear sky', 'id': 800, 'main': 'Clear', 'icon': '01n'}], 'main': {'temp_max': 294.82, 'humidity': 56, 'temp': 293.56, 'temp_min': 292.59, 'pressure': 1020}, 'id': 4699066}
```

2. Прогноз погоды в Сан-Антонио:
```
query_class.query_execution(1, 'by_city_name', ["San Antonio", "US"])
{'cnt': 39, 'cod': '200', 'city': {'country': 'US', 'id': 4726206, 'name': 'San Antonio', 'population': 1327407, 'coord': {'lat': 29.4246, 'lon': -98.4952}}, 'list': [{'sys': {'pod': 'n'}, 'weather': [{'description': 'clear sky', 'id': 800, 'main': 'Clear', 'icon': '01n'}], 'dt_txt': '2019-03-29 03:00:00', 'wind': {'deg': 149.001, 'speed': 6.01}, 'dt': 1553828400, 'main': {'grnd_level': 992.88, 'sea_level': 1017.58, 'humidity': 44, 'temp_max': 294.937, 'temp_min': 291.42, 'pressure': 1017.58, 'temp': 291.42, 'temp_kf': -3.52}, 'clouds': {'all': 0}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'broken clouds', 'id': 803, 'main': 'Clouds', 'icon': '04n'}], 'dt_txt': '2019-03-29 06:00:00', 'wind': {'deg': 147.009, 'speed': 5.22}, 'dt': 1553839200, 'main': {'grnd_level': 993.12, 'sea_level': 1017.93, 'humidity': 68, 'temp_max': 291.85, 'temp_min': 289.21, 'pressure': 1017.93, 'temp': 289.21, 'temp_kf': -2.64}, 'clouds': {'all': 80}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'broken clouds', 'id': 803, 'main': 'Clouds', 'icon': '04n'}], 'dt_txt': '2019-03-29 09:00:00', 'wind': {'deg': 161.005, 'speed': 3.55}, 'dt': 1553850000, 'main': {'grnd_level': 991.64, 'sea_level': 1016.39, 'humidity': 83, 'temp_max': 290.285, 'temp_min': 288.53, 'pressure': 1016.39, 'temp': 288.53, 'temp_kf': -1.76}, 'clouds': {'all': 68}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'light rain', 'id': 500, 'main': 'Rain', 'icon': '10n'}], 'rain': {'3h': 0.01}, 'wind': {'deg': 163.002, 'speed': 3.31}, 'dt': 1553860800, 'dt_txt': '2019-03-29 12:00:00', 'main': {'grnd_level': 990.96, 'sea_level': 1015.81, 'humidity': 83, 'temp_max': 290.872, 'temp_min': 289.99, 'pressure': 1015.81, 'temp': 289.99, 'temp_kf': -0.88}, 'clouds': {'all': 92}}, {'sys': {'pod': 'd'}, 'weather': [{'description': 'light rain', 'id': 500, 'main': 'Rain', 'icon': '10d'}], 'rain': {'3h': 0.04}, 'wind': {'deg': 169.507, 'speed': 4.56}, 'dt': 1553871600, 'dt_txt': '2019-03-29 15:00:00', 'main': {'grnd_level': 991.69, 'sea_level': 1016.44, 'humidity': 75, 'temp_max': 293.08, 'temp_min': 293.08, 'pressure': 1016.44, 'temp': 293.08, 'temp_kf': 0}, 'clouds': {'all': 76}}, {'sys': {'pod': 'd'}, 'weather': [{'description': 'scattered clouds', 'id': 802, 'main': 'Clouds', 'icon': '03d'}], 'rain': {}, 'wind': {'deg': 180.003, 'speed': 5.47}, 'dt': 1553882400, 'dt_txt': '2019-03-29 18:00:00', 'main': {'grnd_level': 991.14, 'sea_level': 1015.55, 'humidity': 55, 'temp_max': 297.494, 'temp_min': 297.494, 'pressure': 1015.55, 'temp': 297.494, 'temp_kf': 0}, 'clouds': {'all': 44}}, {'sys': {'pod': 'd'}, 'weather': [{'description': 'few clouds', 'id': 801, 'main': 'Clouds', 'icon': '02d'}], 'rain': {}, 'wind': {'deg': 180.002, 'speed': 5.22}, 'dt': 1553893200, 'dt_txt': '2019-03-29 21:00:00', 'main': {'grnd_level': 988.32, 'sea_level': 1012.55, 'humidity': 44, 'temp_max': 301.036, 'temp_min': 301.036, 'pressure': 1012.55, 'temp': 301.036, 'temp_kf': 0}, 'clouds': {'all': 12}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'clear sky', 'id': 800, 'main': 'Clear', 'icon': '01n'}], 'rain': {}, 'wind': {'deg': 179, 'speed': 5.25}, 'dt': 1553904000, 'dt_txt': '2019-03-30 00:00:00', 'main': {'grnd_level': 986.85, 'sea_level': 1011.2, 'humidity': 38, 'temp_max': 301.402, 'temp_min': 301.402, 'pressure': 1011.2, 'temp': 301.402, 'temp_kf': 0}, 'clouds': {'all': 0}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'clear sky', 'id': 800, 'main': 'Clear', 'icon': '01n'}], 'rain': {}, 'wind': {'deg': 165.006, 'speed': 5.83}, 'dt': 1553914800, 'dt_txt': '2019-03-30 03:00:00', 'main': {'grnd_level': 988.25, 'sea_level': 1012.78, 'humidity': 53, 'temp_max': 297.04, 'temp_min': 297.04, 'pressure': 1012.78, 'temp': 297.04, 'temp_kf': 0}, 'clouds': {'all': 0}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'clear sky', 'id': 800, 'main': 'Clear', 'icon': '01n'}], 'rain': {}, 'wind': {'deg': 167.01, 'speed': 4.73}, 'dt': 1553925600, 'dt_txt': '2019-03-30 06:00:00', 'main': {'grnd_level': 989.11, 'sea_level': 1013.73, 'humidity': 67, 'temp_max': 293.685, 'temp_min': 293.685, 'pressure': 1013.73, 'temp': 293.685, 'temp_kf': 0}, 'clouds': {'all': 0}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'clear sky', 'id': 800, 'main': 'Clear', 'icon': '01n'}], 'rain': {}, 'wind': {'deg': 173, 'speed': 3.92}, 'dt': 1553936400, 'dt_txt': '2019-03-30 09:00:00', 'main': {'grnd_level': 987.92, 'sea_level': 1012.64, 'humidity': 81, 'temp_max': 291.237, 'temp_min': 291.237, 'pressure': 1012.64, 'temp': 291.237, 'temp_kf': 0}, 'clouds': {'all': 0}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'clear sky', 'id': 800, 'main': 'Clear', 'icon': '01n'}], 'rain': {}, 'wind': {'deg': 175.506, 'speed': 2.36}, 'dt': 1553947200, 'dt_txt': '2019-03-30 12:00:00', 'main': {'grnd_level': 988.29, 'sea_level': 1013.17, 'humidity': 92, 'temp_max': 288.768, 'temp_min': 288.768, 'pressure': 1013.17, 'temp': 288.768, 'temp_kf': 0}, 'clouds': {'all': 0}}, {'sys': {'pod': 'd'}, 'weather': [{'description': 'clear sky', 'id': 800, 'main': 'Clear', 'icon': '01d'}], 'rain': {}, 'wind': {'deg': 195.501, 'speed': 2.06}, 'dt': 1553958000, 'dt_txt': '2019-03-30 15:00:00', 'main': {'grnd_level': 990.05, 'sea_level': 1014.64, 'humidity': 71, 'temp_max': 294.104, 'temp_min': 294.104, 'pressure': 1014.64, 'temp': 294.104, 'temp_kf': 0}, 'clouds': {'all': 0}}, {'sys': {'pod': 'd'}, 'weather': [{'description': 'broken clouds', 'id': 803, 'main': 'Clouds', 'icon': '04d'}], 'rain': {}, 'wind': {'deg': 217.003, 'speed': 2.1}, 'dt': 1553968800, 'dt_txt': '2019-03-30 18:00:00', 'main': {'grnd_level': 989.8, 'sea_level': 1014.24, 'humidity': 56, 'temp_max': 300.626, 'temp_min': 300.626, 'pressure': 1014.24, 'temp': 300.626, 'temp_kf': 0}, 'clouds': {'all': 56}}, {'sys': {'pod': 'd'}, 'weather': [{'description': 'scattered clouds', 'id': 802, 'main': 'Clouds', 'icon': '03d'}], 'rain': {}, 'wind': {'deg': 28.5003, 'speed': 2.29}, 'dt': 1553979600, 'dt_txt': '2019-03-30 21:00:00', 'main': {'grnd_level': 988.17, 'sea_level': 1012.64, 'humidity': 48, 'temp_max': 303.153, 'temp_min': 303.153, 'pressure': 1012.64, 'temp': 303.153, 'temp_kf': 0}, 'clouds': {'all': 48}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'light rain', 'id': 500, 'main': 'Rain', 'icon': '10n'}], 'rain': {'3h': 1.27}, 'wind': {'deg': 30.0039, 'speed': 6.66}, 'dt': 1553990400, 'dt_txt': '2019-03-31 00:00:00', 'main': {'grnd_level': 989.64, 'sea_level': 1014.18, 'humidity': 42, 'temp_max': 299.325, 'temp_min': 299.325, 'pressure': 1014.18, 'temp': 299.325, 'temp_kf': 0}, 'clouds': {'all': 68}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'overcast clouds', 'id': 804, 'main': 'Clouds', 'icon': '04n'}], 'rain': {}, 'wind': {'deg': 29.5006, 'speed': 7.42}, 'dt': 1554001200, 'dt_txt': '2019-03-31 03:00:00', 'main': {'grnd_level': 993.62, 'sea_level': 1018.3, 'humidity': 39, 'temp_max': 294.467, 'temp_min': 294.467, 'pressure': 1018.3, 'temp': 294.467, 'temp_kf': 0}, 'clouds': {'all': 88}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'overcast clouds', 'id': 804, 'main': 'Clouds', 'icon': '04n'}], 'rain': {}, 'wind': {'deg': 25.0014, 'speed': 7.78}, 'dt': 1554012000, 'dt_txt': '2019-03-31 06:00:00', 'main': {'grnd_level': 996.96, 'sea_level': 1021.91, 'humidity': 39, 'temp_max': 290.966, 'temp_min': 290.966, 'pressure': 1021.91, 'temp': 290.966, 'temp_kf': 0}, 'clouds': {'all': 88}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'light rain', 'id': 500, 'main': 'Rain', 'icon': '10n'}], 'rain': {'3h': 0.72}, 'wind': {'deg': 23.5093, 'speed': 8.76}, 'dt': 1554022800, 'dt_txt': '2019-03-31 09:00:00', 'main': {'grnd_level': 997.87, 'sea_level': 1023.3, 'humidity': 45, 'temp_max': 287.604, 'temp_min': 287.604, 'pressure': 1023.3, 'temp': 287.604, 'temp_kf': 0}, 'clouds': {'all': 88}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'light rain', 'id': 500, 'main': 'Rain', 'icon': '10n'}], 'rain': {'3h': 0.54}, 'wind': {'deg': 23.5009, 'speed': 8.33}, 'dt': 1554033600, 'dt_txt': '2019-03-31 12:00:00', 'main': {'grnd_level': 999.62, 'sea_level': 1025.29, 'humidity': 48, 'temp_max': 285, 'temp_min': 285, 'pressure': 1025.29, 'temp': 285, 'temp_kf': 0}, 'clouds': {'all': 92}}, {'sys': {'pod': 'd'}, 'weather': [{'description': 'broken clouds', 'id': 803, 'main': 'Clouds', 'icon': '04d'}], 'rain': {}, 'wind': {'deg': 26.5005, 'speed': 8.06}, 'dt': 1554044400, 'dt_txt': '2019-03-31 15:00:00', 'main': {'grnd_level': 1001.65, 'sea_level': 1027.4, 'humidity': 46, 'temp_max': 284.621, 'temp_min': 284.621, 'pressure': 1027.4, 'temp': 284.621, 'temp_kf': 0}, 'clouds': {'all': 76}}, {'sys': {'pod': 'd'}, 'weather': [{'description': 'clear sky', 'id': 800, 'main': 'Clear', 'icon': '02d'}], 'rain': {}, 'wind': {'deg': 30.0001, 'speed': 5.91}, 'dt': 1554055200, 'dt_txt': '2019-03-31 18:00:00', 'main': {'grnd_level': 1001.72, 'sea_level': 1027.12, 'humidity': 47, 'temp_max': 289.191, 'temp_min': 289.191, 'pressure': 1027.12, 'temp': 289.191, 'temp_kf': 0}, 'clouds': {'all': 8}}, {'sys': {'pod': 'd'}, 'weather': [{'description': 'clear sky', 'id': 800, 'main': 'Clear', 'icon': '01d'}], 'rain': {}, 'wind': {'deg': 29.5056, 'speed': 3.41}, 'dt': 1554066000, 'dt_txt': '2019-03-31 21:00:00', 'main': {'grnd_level': 999.58, 'sea_level': 1024.76, 'humidity': 41, 'temp_max': 292.163, 'temp_min': 292.163, 'pressure': 1024.76, 'temp': 292.163, 'temp_kf': 0}, 'clouds': {'all': 0}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'overcast clouds', 'id': 804, 'main': 'Clouds', 'icon': '04n'}], 'rain': {}, 'wind': {'deg': 32.503, 'speed': 3.97}, 'dt': 1554076800, 'dt_txt': '2019-04-01 00:00:00', 'main': {'grnd_level': 998.23, 'sea_level': 1023.47, 'humidity': 29, 'temp_max': 291.127, 'temp_min': 291.127, 'pressure': 1023.47, 'temp': 291.127, 'temp_kf': 0}, 'clouds': {'all': 88}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'few clouds', 'id': 801, 'main': 'Clouds', 'icon': '02n'}], 'rain': {}, 'wind': {'deg': 51.5018, 'speed': 3.96}, 'dt': 1554087600, 'dt_txt': '2019-04-01 03:00:00', 'main': {'grnd_level': 999.58, 'sea_level': 1024.97, 'humidity': 31, 'temp_max': 288.486, 'temp_min': 288.486, 'pressure': 1024.97, 'temp': 288.486, 'temp_kf': 0}, 'clouds': {'all': 20}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'few clouds', 'id': 801, 'main': 'Clouds', 'icon': '02n'}], 'rain': {}, 'wind': {'deg': 48.5, 'speed': 3.27}, 'dt': 1554098400, 'dt_txt': '2019-04-01 06:00:00', 'main': {'grnd_level': 1000.53, 'sea_level': 1026.17, 'humidity': 37, 'temp_max': 285.386, 'temp_min': 285.386, 'pressure': 1026.17, 'temp': 285.386, 'temp_kf': 0}, 'clouds': {'all': 24}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'broken clouds', 'id': 803, 'main': 'Clouds', 'icon': '04n'}], 'rain': {}, 'wind': {'deg': 33.5005, 'speed': 3.51}, 'dt': 1554109200, 'dt_txt': '2019-04-01 09:00:00', 'main': {'grnd_level': 1000.15, 'sea_level': 1025.86, 'humidity': 38, 'temp_max': 284.87, 'temp_min': 284.87, 'pressure': 1025.86, 'temp': 284.87, 'temp_kf': 0}, 'clouds': {'all': 64}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'light rain', 'id': 500, 'main': 'Rain', 'icon': '10n'}], 'rain': {'3h': 0.16}, 'wind': {'deg': 25, 'speed': 3.8}, 'dt': 1554120000, 'dt_txt': '2019-04-01 12:00:00', 'main': {'grnd_level': 1001.25, 'sea_level': 1026.95, 'humidity': 46, 'temp_max': 283.953, 'temp_min': 283.953, 'pressure': 1026.95, 'temp': 283.953, 'temp_kf': 0}, 'clouds': {'all': 92}}, {'sys': {'pod': 'd'}, 'weather': [{'description': 'light rain', 'id': 500, 'main': 'Rain', 'icon': '10d'}], 'rain': {'3h': 0.03}, 'wind': {'deg': 28.0062, 'speed': 4.01}, 'dt': 1554130800, 'dt_txt': '2019-04-01 15:00:00', 'main': {'grnd_level': 1002.97, 'sea_level': 1028.7, 'humidity': 47, 'temp_max': 284.679, 'temp_min': 284.679, 'pressure': 1028.7, 'temp': 284.679, 'temp_kf': 0}, 'clouds': {'all': 92}}, {'sys': {'pod': 'd'}, 'weather': [{'description': 'overcast clouds', 'id': 804, 'main': 'Clouds', 'icon': '04d'}], 'rain': {}, 'wind': {'deg': 49.0067, 'speed': 3.22}, 'dt': 1554141600, 'dt_txt': '2019-04-01 18:00:00', 'main': {'grnd_level': 1002.81, 'sea_level': 1028.32, 'humidity': 40, 'temp_max': 286.659, 'temp_min': 286.659, 'pressure': 1028.32, 'temp': 286.659, 'temp_kf': 0}, 'clouds': {'all': 88}}, {'sys': {'pod': 'd'}, 'weather': [{'description': 'few clouds', 'id': 801, 'main': 'Clouds', 'icon': '02d'}], 'rain': {}, 'wind': {'deg': 69.5001, 'speed': 2.66}, 'dt': 1554152400, 'dt_txt': '2019-04-01 21:00:00', 'main': {'grnd_level': 1000.15, 'sea_level': 1025.43, 'humidity': 43, 'temp_max': 290.343, 'temp_min': 290.343, 'pressure': 1025.43, 'temp': 290.343, 'temp_kf': 0}, 'clouds': {'all': 20}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'clear sky', 'id': 800, 'main': 'Clear', 'icon': '01n'}], 'rain': {}, 'wind': {'deg': 87.0033, 'speed': 2.41}, 'dt': 1554163200, 'dt_txt': '2019-04-02 00:00:00', 'main': {'grnd_level': 999.04, 'sea_level': 1024.21, 'humidity': 36, 'temp_max': 290.547, 'temp_min': 290.547, 'pressure': 1024.21, 'temp': 290.547, 'temp_kf': 0}, 'clouds': {'all': 0}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'clear sky', 'id': 800, 'main': 'Clear', 'icon': '01n'}], 'rain': {}, 'wind': {'deg': 136.004, 'speed': 2.77}, 'dt': 1554174000, 'dt_txt': '2019-04-02 03:00:00', 'main': {'grnd_level': 1000.16, 'sea_level': 1025.77, 'humidity': 57, 'temp_max': 284.241, 'temp_min': 284.241, 'pressure': 1025.77, 'temp': 284.241, 'temp_kf': 0}, 'clouds': {'all': 0}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'scattered clouds', 'id': 802, 'main': 'Clouds', 'icon': '03n'}], 'rain': {}, 'wind': {'deg': 150.005, 'speed': 2.43}, 'dt': 1554184800, 'dt_txt': '2019-04-02 06:00:00', 'main': {'grnd_level': 1000.64, 'sea_level': 1026.48, 'humidity': 83, 'temp_max': 281.496, 'temp_min': 281.496, 'pressure': 1026.48, 'temp': 281.496, 'temp_kf': 0}, 'clouds': {'all': 32}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'clear sky', 'id': 800, 'main': 'Clear', 'icon': '02n'}], 'rain': {}, 'wind': {'deg': 142, 'speed': 1.22}, 'dt': 1554195600, 'dt_txt': '2019-04-02 09:00:00', 'main': {'grnd_level': 999.89, 'sea_level': 1025.84, 'humidity': 89, 'temp_max': 278.768, 'temp_min': 278.768, 'pressure': 1025.84, 'temp': 278.768, 'temp_kf': 0}, 'clouds': {'all': 8}}, {'sys': {'pod': 'n'}, 'weather': [{'description': 'few clouds', 'id': 801, 'main': 'Clouds', 'icon': '02n'}], 'rain': {}, 'wind': {'deg': 69.5004, 'speed': 1.22}, 'dt': 1554206400, 'dt_txt': '2019-04-02 12:00:00', 'main': {'grnd_level': 999.46, 'sea_level': 1025.46, 'humidity': 93, 'temp_max': 277.386, 'temp_min': 277.386, 'pressure': 1025.46, 'temp': 277.386, 'temp_kf': 0}, 'clouds': {'all': 20}}, {'sys': {'pod': 'd'}, 'weather': [{'description': 'few clouds', 'id': 801, 'main': 'Clouds', 'icon': '02d'}], 'rain': {}, 'wind': {'deg': 85.5007, 'speed': 1.96}, 'dt': 1554217200, 'dt_txt': '2019-04-02 15:00:00', 'main': {'grnd_level': 1000.42, 'sea_level': 1025.88, 'humidity': 61, 'temp_max': 287.517, 'temp_min': 287.517, 'pressure': 1025.88, 'temp': 287.517, 'temp_kf': 0}, 'clouds': {'all': 12}}, {'sys': {'pod': 'd'}, 'weather': [{'description': 'few clouds', 'id': 801, 'main': 'Clouds', 'icon': '02d'}], 'rain': {}, 'wind': {'deg': 129.5, 'speed': 3.92}, 'dt': 1554228000, 'dt_txt': '2019-04-02 18:00:00', 'main': {'grnd_level': 999.17, 'sea_level': 1024.07, 'humidity': 49, 'temp_max': 292.757, 'temp_min': 292.757, 'pressure': 1024.07, 'temp': 292.757, 'temp_kf': 0}, 'clouds': {'all': 24}}, {'sys': {'pod': 'd'}, 'weather': [{'description': 'broken clouds', 'id': 803, 'main': 'Clouds', 'icon': '04d'}], 'rain': {}, 'wind': {'deg': 147.504, 'speed': 4.66}, 'dt': 1554238800, 'dt_txt': '2019-04-02 21:00:00', 'main': {'grnd_level': 995.95, 'sea_level': 1020.72, 'humidity': 37, 'temp_max': 294.408, 'temp_min': 294.408, 'pressure': 1020.72, 'temp': 294.408, 'temp_kf': 0}, 'clouds': {'all': 56}}], 'message': 0.0075}
```
