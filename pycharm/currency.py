import warnings
import requests
from database import DatabaseManager


def get_currency(try_counter=1):

    # Проверка количества попыток для подключения (максимум 4)
    if try_counter >= 4:
        warnings.warn("Попытки подключения к API мосбиржи оказались неудачными", RuntimeWarning)
        return

    url = "https://iss.moex.com/iss/engines/currency/markets/index/securities.json"
    response = requests.get(url)  # запрос данных по url

    # Проверка успешного подключения
    if response.status_code != 200:
        warnings.warn("Не удалось подключиться к API мосбиржи", RuntimeWarning)

        # Выполняем повторное подключение
        get_currency(try_counter = try_counter + 1)
        return

    data = response.json()  # Преобразование ответа в JSON

    # количество возможных для парсинга валют
    str_number = min(len(data['securities']['data']), len(data['marketdata']['data']))

    inf = {}

    # проходим по всем валютам
    for i in range(str_number):

        try:
            # BOARDID
            inf[data['securities']['columns'][0]] = data['securities']['data'][i][0]

            # SECID
            inf[data['securities']['columns'][1]] = data['securities']['data'][i][1]

            # SHORTNAME
            inf[data['securities']['columns'][2]] = data['securities']['data'][i][2]

            # LATNAME
            inf[data['securities']['columns'][3]] = data['securities']['data'][i][3]

            # NAME
            inf[data['securities']['columns'][4]] = data['securities']['data'][i][4]

            # TRADEDATE
            inf[data['marketdata']['columns'][2]] = data['marketdata']['data'][i][2]

            # TIME
            inf[data['marketdata']['columns'][3]] = data['marketdata']['data'][i][3]

            # LASTVALUE
            inf[data['marketdata']['columns'][4]] = data['marketdata']['data'][i][4]
        except:
            print(f"Ошибка при загрузке валют")
            warnings.warn(f"Информация не найдена", UserWarning)
            return inf

        # Сохранение в базу данных
        db = DatabaseManager('bonds.db')
        db.insert_dict("currency", inf)

    print("Данные о валютах обновлены")