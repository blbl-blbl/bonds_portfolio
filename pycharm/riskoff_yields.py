import polars as pl
import requests
from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime
from fake_useragent import UserAgent
import re
import yfinance as yf


def get_riskoff_yeilds(currency):
    # В зависимости от валюты выбирабтся безрисковые доходности

    if currency == 'RUB':
        df = rub_yield()
        return df
    elif currency == 'USD':
        df = usd_yield()
        return df
    elif currency == 'CNY':
        df = cny_yield()
        return df
    elif currency == 'EUR':
        df = euro_yield()
        return df
    else:
        df = pl.DataFrame()
        return df


def rub_yield():
    # Получение безрисковых ставок с api мосбиржи

    from df_process import dataframe_process

    today = date.today()
    day_counter = 0

    url = f'https://iss.moex.com/iss/engines/stock/zcyc.json?date={today}'
    response = requests.get(url)  # запрос данных по url
    data = response.json()  # Преобразование ответа в JSON

    # если выходной или праздник, то в этот день нет данных - пропускаем его и идем дальше
    while not data['yearyields']['data'] and day_counter < 30:
        today = today - timedelta(days=1)
        url = f'https://iss.moex.com/iss/engines/stock/zcyc.json?date={today}'
        response = requests.get(url)  # запрос данных по url
        data = response.json()  # Преобразование ответа в JSON

    # Названия столбцов
    schema = data['yearyields']['columns']

    # Данные
    lsts = [[], [], [], []]

    for i in range(len(data['yearyields']['data'])):
        lsts[0].append(data['yearyields']['data'][i][0])
        lsts[1].append(data['yearyields']['data'][i][1])
        lsts[2].append(data['yearyields']['data'][i][2])
        lsts[3].append(data['yearyields']['data'][i][3])

    # Создание датафрейма polars
    df = pl.DataFrame(lsts, schema=['tradedate', 'tradetime', 'period', 'value'], orient="col")

    # обработка датафрейма (преобразование в дату и удаление лишних столбцов)
    df = dataframe_process(df, date_columns=['tradedate'], drop_columns=['tradetime', 'tradedate_delta'])

    return df


def usd_yield():
    # Тикеры для основных сроков
    tickers = {
        '0.25': '^IRX',
        '0.50': '^IRX',
        '1.00': '^TNX',
        '2.00': '^TNX',
        '5.00': '^FVX',
        '10.00': '^TNX',
        '30.00': '^TYX'
    }
    data_records = []

    for maturity, ticker in tickers.items():
        try:
            bond_data = yf.Ticker(ticker)
            hist = bond_data.history(period='1d')

            if not hist.empty:
                current_yield = hist['Close'].iloc[-1]

                data_records.append({
                    'period': maturity,
                    'ticker': ticker,
                    'value': current_yield,
                    'date': datetime.now().date(),
                })

        except Exception as e:
            print(f"Ошибка для {maturity}: {e}")

    # Создаем DataFrame Polars
    if data_records:
        df = pl.DataFrame(data_records)
    else:
        df = pl.DataFrame()

    return df


def cny_yield():
    # Получение безрисковой ставки для юаней (CNY)

    try:
        url = 'https://yield.chinabond.com.cn/cbweb-czb-web/czb/moreInfo?locale=en_US&nameType=1'
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'lxml')
    except:
        print()
        print("!!! НЕ УДАЛОСЬ ПОДКЛЮЧИТЬСЯ К САЙТУ ДЛЯ ПАРСИНГА БЕЗРИСКОВОЙ ДОХОДНОСТИ ДЛЯ ЮАНЕЙ !!!")
        print()
        return

    try:
        soup1 = soup.find('div', id='gjqxData')
        trs = soup1.find_all('tr')

        data = []

        for i in trs:
            temp = []
            for j in i.find_all('td'):
                # Проверка формата YYYY-MM-DD (строку с датой не добавляем )
                pattern = r'^\d{4}-\d{2}-\d{2}$'
                if re.match(pattern, j.text) or j.text == 'Date':
                    continue
                else:
                    # print(j.text)
                    temp.append(j.text)
            data.append(temp)
    except:
        print()
        print("!!! НЕ УДАЛОСЬ ОБРАБОТАТЬ HTML ДЛЯ ПАРСИНГА БЕЗРИСКОВОЙ ДОХОДНОСТИ ДЛЯ ЮАНЕЙ !!!")
        print()
        return

    titles = data[0]
    data = data[1:]

    df = pl.DataFrame(data, schema=titles, orient='row')

    # Переименовываем столбцы
    df = df.rename({"Maturity": "period", "Yield(%)": "value"})

    # Замена старых названий для периода
    true_period = [0.25, 0.5, 1.00, 2.00, 3.00, 5.00, 7.00, 10.00, 30.00]

    df = df.with_columns(
        pl.Series('period', true_period)
    )

    # Конвератция доходности из string в float
    df = df.cast({"value": pl.Float32})

    return df


def euro_yield():
    # Парсинг безрисковой ставки по ЕВРО с сайта Investing.com
    # В качестве безрисковой выбрана доходность государственных облигаций Германии

    # Класс для создания рандомного useragent
    ua = UserAgent()

    # Сбор HTML с сайта Investing
    try:
        with requests.Session() as session:
            session.headers.update({
                'User-Agent': ua.random,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            })
            response = session.get("https://www.investing.com/rates-bonds/germany-government-bonds")

        soup = BeautifulSoup(response.text, 'lxml')
    except:
        print()
        print("!!! НЕ УДАЛОСЬ ПОДКЛЮЧИТЬСЯ К САЙТУ INVESING.COM !!!")
        print()
        raise

    try:
        # Нахождение нужного блока
        soup1 = soup.find('table', class_='genTbl closedTbl crossRatesTbl')
        soup2 = soup1.find('tbody')
        soup3 = soup2.find_all('tr')

        names = []  # Названия периода
        values = []  # Доходности для периода

        for i in soup3:
            name = i.find_all('td')[1].text  # Сбор названия периода
            value = i.find_all('td')[2].text  # Сбор доходности
            names.append(name)
            values.append(value)
    except:
        print()
        print("!!! НЕ УДАЛОСЬ НАЙТИ НУЖНЫЙ БЛОК В HTML !!!")
        print()
        raise

    # Соответствие периодов
    corres_table = {
        'Germany 3M': 0.25,
        'Germany 6M': 0.5,
        'Germany 9M': 0.75,
        'Germany 1Y': 1.00,
        'Germany 2Y': 2.00,
        'Germany 3Y': 3.00,
        'Germany 4Y': 4.00,
        'Germany 5Y': 5.00,
        'Germany 6Y': 6.00,
        'Germany 7Y': 7.00,
        'Germany 8Y': 8.00,
        'Germany 9Y': 9.00,
        'Germany 10Y': 10.00,
        'Germany 15Y': 15.00,
        'Germany 20Y': 20.00,
        'Germany 25Y': 25.00,
        'Germany 30Y': 30.00}

    # Применяем соответствие периодов
    true_period = [corres_table[item] if item in corres_table else item for item in names]

    if not true_period:
        print()
        print("!!! ОШИБКА В ТАБИЦЕ КОРРЕКТИРОВКИ !!!")
        print()
        raise

    data = {'period': true_period, 'value': values}
    df = pl.DataFrame(data)
    df = df.cast({"value": pl.Float32})

    return df
