import polars as pl
from database import DatabaseManager
from marketdata import get_marketdata
from datetime import date
import warnings
from visualization import create_monthly_dict, fill_calendar_with_sums, plot_coupon_calendar_seaborn, freerisk_plot


def portfolio_upload(path):
    # Загрузка портфеля облигаций из эксель файла. Файл содержит 2 столбца: ISIN'ы и доля каждого isin
    try:
        df = pl.read_excel(path)
    except IOError as e:
        print(f"Не найден файл по пути {path}")
        raise e

    try:
        df.cast({'Количество лотов': pl.Int32})
    except:
        print("В столбце 'Количество лотов' должны быть только целочисловые значения")
        raise ValueError

    if len(df.columns) != 2:
        print(
            "Файл эксель должен состоять из 2 столбцов: 'ISIN' (строковое) и 'Количество лотов' (float) каждого из ISIN в портфеле")
        return

    # Подключение к базе данных
    db = DatabaseManager('bonds.db')

    # Удаление старой таблицы с данными по облигациям
    db.delete_table("bonds_info")

    # обновление данных по каждому ISIN в базе данных
    for row in df.iter_rows():
        get_marketdata(row[0])

    # Уникальные ISIN из датафрейма
    unique_isins = df["ISIN"].unique().to_list()

    bond_data_df = db.fetch_data_from_sqlite(df, unique_isins, "bonds_info", "ISIN")

    df = dataframe_process(bond_data_df,
                           date_columns=['NEXTCOUPON', 'MATDATE', 'YIELDDATE', 'OFFERDATE'],
                           drop_columns=['SECID', 'BOARDID', 'id', 'ISSUESIZE'])

    # Уникальные валюты в портфеле
    unique_currency = df['FACEUNIT'].unique().to_list()

    df = add_currency_rub(df)

    # Преобразуем количество бумаг в долю в портфеле (по стоимости)
    df = get_share(df)

    # return df

    # Для каждой валюты вычисляем характеристики портфеля
    for currency in unique_currency:  # сюда поставить unique_currency вместо list
        filtered_df = df.filter(pl.col('FACEUNIT') == currency)
        if round(filtered_df['Доля'].sum(), 5) > 0:
            # print(filtered_df)
            portfolio_info(filtered_df, currency)

    # Дата погашения самой "длинной" облигации
    end_date = max(df['MATDATE'])

    # пустой календарь с текущей даты по end_date
    calendar = create_monthly_dict(end_date)

    # Заполнение календаря (учитываются купоны и погашение)
    payment_calendar = fill_calendar_with_sums(calendar_dict=calendar, df=df, end_date=end_date)

    # Построение графика с выплатами
    plot_coupon_calendar_seaborn(calendar_dict=payment_calendar)

    return df


def get_share(df):
    # Добавляет в датафрейм столбец с долей каждой бумаги

    try:
        df.cast({'Количество лотов': pl.Int32})
    except:
        print("В столбце 'Доля' должны быть только числовые значения")
        raise ValueError

    # Создание стоблца с оставшимися днями до выплаты купона в формате int
    df = df.with_columns(
        pl.col('NEXTCOUPON_delta').dt.total_days().alias('NEXTCOUPON_delta_int')
    )

    # Расчет полной стоимости каждой облигации в портфеле
    # В идеале брать курс валют ЦБ для правильного расчета, но пока что беру что есть
    df = df.with_columns(
        (pl.col('FACEVALUE') * pl.coalesce(pl.col('LAST'), pl.col('MARKETPRICE')) / 100 * pl.col(
            'CURRENCY_RUB') * pl.col('Количество лотов') * pl.col('LOTSIZE') +
         (pl.col('COUPONPERIOD') - pl.col('NEXTCOUPON_delta_int')) / pl.col('COUPONPERIOD') * pl.col(
                    'COUPONVALUE') * pl.col('CURRENCY_RUB'))
        .alias('FULLVALUE_RUB'))

    full_sum = df['FULLVALUE_RUB'].sum()

    df = df.with_columns(
        (pl.col('FULLVALUE_RUB') / full_sum).alias('Доля')
    )

    return df


def dataframe_process(df, date_columns: list = [], drop_columns=[]):
    # обработка датафрейма

    today = date.today()

    try:
        for column in date_columns:
            if column:
                df = df.cast({column: pl.Date})
                # Добавление столбцов: дата - текущая дата для всех столбцов где есть даты
                delta_days = (pl.Series(df[column]) - today).alias(f"{column}_delta")
                column_index = df.get_column_index(column)
                df.insert_column(column_index + 1, delta_days)

    except:
        warnings.warn(f"Возникла ошибка с трансформацией строки в дату.", UserWarning)
        raise

    try:
        for column in drop_columns:
            df = df.drop(column)
    except:
        warnings.warn(f"Возникла ошибка с при удалении столбца.", UserWarning)

    return df


def add_currency_rub(df):
    # Добавление стоимости валюты в рублях для каждой облигации

    # Уникальные валюты в датафрейме
    unique_currency = df['FACEUNIT'].unique().to_list()

    # Получение значения валюты в рублях для каждой уникльной валюты в датафрейме
    db = DatabaseManager('bonds.db')
    values = []
    for currency in unique_currency:
        value = db.currency_value(currency)
        if value == 1:  # обработка для рублей
            values.append(float(value))
        elif value is not None:  # если валюта найдена
            values.append(float(value[0]))
        else:
            values.append(0)
            print(f"Значение для валюты {currency} не найдены!")

    # Словарь валюта : значение в рублях
    currencies = dict(zip(unique_currency, values))

    # Вставка столбца в датафрейм
    df = df.with_columns(
        pl.col('FACEUNIT').replace(currencies).alias('CURRENCY_RUB')
    )

    # Преобразование столбца в flaot формат
    df = df.cast({'CURRENCY_RUB': pl.Float32})

    return df

def portfolio_info(df, currency):
    # Расчет показателей портфеля

    weighted_YTM = 0.0
    weighted_duration = 0.0
    weighted_couponperid = 0.0
    weighted_couponpercent = 0.0
    weighted_yield = 0.0
    weighted_maturity_date = 0.0

    # пересчет долей
    sum_share = df['Доля'].sum()
    df = df.with_columns(
        (pl.col('Доля') / sum_share).alias('Доля')  # то же имя столбца
    )


    for row in df.iter_rows(named=True):
        # взвешенный купонный период
        weighted_couponperid += row['Доля'] * row['COUPONPERIOD']

        # взвешенный YTM
        weighted_YTM += row['Доля'] * row['EFFECTIVEYIELD']

        # взвешенная дюрация
        weighted_duration += row['Доля'] * row['DURATION']

        # взвешенный процент по купонам
        weighted_couponpercent += row['Доля'] * row['COUPONPERCENT']

        # взвешенная доходность
        weighted_yield += row['Доля'] * row['YIELD']

        # Взвешенный срок до погашения
        weighted_maturity_date += row['Доля'] * (row['MATDATE_delta'].total_seconds() / 86400)

        # # Взвешенный срок до события
        # weighted_maturity_date += row['Доля'] * (row[7].total_seconds() / 86400)

    print(f"Информация по портфелю в валюте {currency}")
    print(f"YTM портфеля: {round(weighted_YTM, 2)}%")
    print(f"Доходность портфеля: {round(weighted_yield, 2)}%")
    print(f"Дюрация портфеля: {round(weighted_duration, 2)} дней")
    print(f"Взвешенный процент по купонам: {round(weighted_couponpercent, 2)}")
    print(f"Взвешенный купонный период по портфелю: {round(weighted_couponperid, 2)} дней")
    print(f"Взвешенный срок до погашения: {round(weighted_maturity_date, 2)} дней")
    print(f"Взвешенный срок до погашения: {round(weighted_maturity_date / 365, 3)} лет")

    freerisk_plot(weighted_maturity_date / 365, weighted_YTM, currency)


