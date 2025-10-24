import requests
import warnings
from database import DatabaseManager


def get_marketdata(isin, try_counter = 1):
    # Подключение к API мосбиржи

    # Проверка количества попыток для подключения (максимум 4)
    if try_counter >= 4:
        warnings.warn("Попытки подключения к API мосбиржи оказались неудачными", RuntimeWarning)
        return

    url = f"https://iss.moex.com/iss/engines/stock/markets/bonds/securities/{isin}.json"
    response = requests.get(url)  # запрос данных по url

    # Проверка успешного подключения
    if response.status_code != 200:
        warnings.warn("Не удалось подключиться к API мосбиржи", RuntimeWarning)

        # Выполняем повторное подключение
        get_marketdata(isin, try_counter = try_counter + 1)
        return

    data = response.json()  # Преобразование ответа в JSON

    # Получение данных из блока securities
    inf = get_securities_block(isin, data)

    # Получение данных из блока marketdata
    inf1 = get_marketdata_block(inf, isin, data)

    # Получение данных из блока marketdata_yields
    inf2 = get_marketdata_yields_block(inf1, isin, data)

    if inf2:
        # Сохранение в базу данных
        db = DatabaseManager('bonds.db')
        db.insert_dict("bonds_info", inf2)
    else:
        print(f"Информация по {isin} не найдена")


def get_securities_block(isin, data) -> dict:
    # Получение данных из блока securities

    inf = {}

    try:
        # Возможно имеет смысл добавить обработку try except для каждого получаемого поля
        # SECID (ISIN)
        inf[data["securities"]["columns"][0]] = data["securities"]["data"][0][0]
        inf[data["securities"]["columns"][0]] = data["securities"]["data"][0][0]

        # get boardid (режим торгов)
        inf[data["securities"]["columns"][1]] = data["securities"]["data"][0][1]

        # значение купона
        inf[data["securities"]["columns"][5]] = data["securities"]["data"][0][5]

        # дата следующего купона
        inf[data["securities"]["columns"][6]] = data["securities"]["data"][0][6]

        # Lotsize (лотность)
        inf[data["securities"]["columns"][9]] = data["securities"]["data"][0][9]

        # facevalue (номинал)
        inf[data["securities"]["columns"][10]] = data["securities"]["data"][0][10]

        # status
        inf[data["securities"]["columns"][12]] = data["securities"]["data"][0][12]

        # matdate (Дата погашения)
        inf[data["securities"]["columns"][13]] = data["securities"]["data"][0][13]

        # COUPONPERIOD
        inf[data["securities"]["columns"][15]] = data["securities"]["data"][0][15]

        # ISSUESIZE
        inf[data["securities"]["columns"][16]] = data["securities"]["data"][0][16]

        # SECNAME
        inf[data["securities"]["columns"][19]] = data["securities"]["data"][0][19]

        # FACEUNIT (валюта)
        inf[data["securities"]["columns"][25]] = data["securities"]["data"][0][25]

        # ISIN
        inf[data["securities"]["columns"][28]] = data["securities"]["data"][0][28]

        # COUPONPERCENT (купон в процентах)
        inf[data["securities"]["columns"][35]] = data["securities"]["data"][0][35]

        # OFFERDATE (дата оферты)
        inf[data["securities"]["columns"][36]] = data["securities"]["data"][0][36]

    except:
        print(f"Ошибка с ISIN {isin} в блоке securities")
        warnings.warn(f"Информация в блоке securities по бумаге {isin} не найдена", UserWarning)

    finally:
        return inf


def get_marketdata_block(inf, isin, data) -> dict:
    # Получение данных из блока marketdata

    try:
        # LAST (последняя цена)
        inf[data["marketdata"]["columns"][27]] = data["marketdata"]["data"][0][27]

        # MARKETPRICE (должна подгружаться даже когда нет торгов)
        inf[data["marketdata"]["columns"][11]] = data["marketdata"]["data"][0][11]

        # VALUE (должна быть цена в рублях)
        inf[data["marketdata"]["columns"][15]] = data["marketdata"]["data"][0][15]

        # YIELD (YTM)
        inf[data["marketdata"]["columns"][16]] = data["marketdata"]["data"][0][16]

        # VALUE_USD (цена в долларах)
        inf[data["marketdata"]["columns"][17]] = data["marketdata"]["data"][0][17]

        # DURATION (в днях)
        inf[data["marketdata"]["columns"][36]] = data["marketdata"]["data"][0][36]

        # YIELDTOOFFER (доходность к оферте)
        inf[data["marketdata"]["columns"][56]] = data["marketdata"]["data"][0][56]

    except:
        print(f"Ошибка с ISIN {isin} в блоке marketdata")
        warnings.warn(f"Информация в блоке marketdata по бумаге {isin} не найдена", UserWarning)

    finally:
        return inf


def get_marketdata_yields_block(inf, isin, data) -> dict:
    # Получение данных из блока marketdata_yields

    try:
        # YIELDDATE (дата на которую рассчитывается доходность)
        inf[data["marketdata_yields"]["columns"][3]] = data["marketdata_yields"]["data"][0][3]

        # YIELDDATETYPE (тип события которое будет в дату на которую рассчитывается доходность)
        inf[data["marketdata_yields"]["columns"][5]] = data["marketdata_yields"]["data"][0][5]

        # EFFECTIVEYIELD (эффективная доходность)
        inf[data["marketdata_yields"]["columns"][6]] = data["marketdata_yields"]["data"][0][6]

        # ZSPREADBP (z спред)
        inf[data["marketdata_yields"]["columns"][8]] = data["marketdata_yields"]["data"][0][8]

        # GSPREADBP (g спред)
        inf[data["marketdata_yields"]["columns"][9]] = data["marketdata_yields"]["data"][0][9]

    except:
        print(f"Ошибка с ISIN {isin} в блоке marketdata_yields")
        warnings.warn(f"Информация в блоке marketdata_yields по бумаге {isin} не найдена", UserWarning)

    finally:
        return inf


