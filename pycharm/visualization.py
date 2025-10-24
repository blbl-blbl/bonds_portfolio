from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import seaborn as sns
import matplotlib.pyplot as plt
import polars as pl
from riskoff_yields import get_riskoff_yeilds


def create_monthly_dict(end_date):
    """
    Ежемесячный календарь с текущего месяца по максимальную в dataframe
    """
    current_year = datetime.now().year
    current_month = datetime.now().month
    start_date = date(current_year, current_month, 1)
    # end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    monthly_dict = {}
    current = start_date

    while current <= end_date:
        monthly_dict[current] = 0
        # Переходим к следующему месяцу
        current += relativedelta(months=1)
        current = current.replace(day=1)  # Обеспечиваем первый день месяца

    return monthly_dict


def fill_calendar_with_sums(calendar_dict, df, end_date):
    """
    Заполняет календарь суммами купонных выплат по месяцам
    Все значения приведены к рублю по текущему курсу

    Args:
        calendar_dict: словарь-календарь {date: 0}
        df: Polars DataFrame с колонками:
            - next_coupon_date: дата ближайшей выплаты
            - days_between_coupons: дни между выплатами
            - coupon_amount: сумма выплаты
        end_date: конечная дата для расчета выплат
    """
    # Создаем копию календаря
    filled_calendar = calendar_dict.copy()

    # Обрабатываем каждую бумагу
    for row in df.iter_rows(named=True):
        coupon_date = row['NEXTCOUPON']
        days_interval = row['COUPONPERIOD']
        coupon_amount = row['COUPONVALUE'] * row['CURRENCY_RUB'] * row['Количество лотов']
        maturity_date = row['MATDATE']
        maturity_sum = row['FACEVALUE'] * row['LOTSIZE'] * row['CURRENCY_RUB'] * row['Количество лотов']

        # Добавляем все будущие купоны до end_date
        while coupon_date <= end_date:
            # Находим первый день месяца для этой даты
            month_key = coupon_date.replace(day=1)

            # Если этот месяц есть в календаре, добавляем сумму
            if month_key in filled_calendar:
                filled_calendar[month_key] += coupon_amount

            if maturity_date.month == coupon_date.month and maturity_date.year == coupon_date.year:
                filled_calendar[month_key] += maturity_sum

            # Переходим к следующей выплате
            coupon_date += timedelta(days=days_interval)

    return filled_calendar


def plot_coupon_calendar_seaborn(calendar_dict, title="График выплат по месяцам"):
    """
    Строит гистограмму купонных выплат по месяцам с использованием Seaborn

    Args:
        calendar_dict: словарь с данными {date: сумма}
        title: заголовок графика
    """
    # Фильтруем только месяцы с ненулевыми выплатами и создаем Polars DataFrame
    data = [
        (date.strftime('%Y-%m'), amount)
        for date, amount in calendar_dict.items()
        if amount != 0
    ]

    if not data:
        print("Нет данных для построения графика")
        return

    df_plot = pl.DataFrame({
        'month': [item[0] for item in data],
        'amount': [item[1] for item in data]
    }).sort('month')

    # Округляем суммы до целых
    df_plot = df_plot.with_columns([
        pl.col('amount').round().cast(pl.Int64)
    ])

    # Создаем график
    plt.figure(figsize=(14, 7))
    ax = sns.barplot(data=df_plot.to_pandas(), x='month', y='amount', color='#3498db')

    # Настройки оформления
    plt.title(title, fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('Месяц', fontsize=12)
    plt.ylabel('Сумма выплат, руб.', fontsize=12)
    plt.xticks(rotation=45, ha='right')

    # Форматирование подписей значений
    for p in ax.patches:
        if p.get_height() > 0:
            ax.annotate(f'{p.get_height():,.0f}',
                        (p.get_x() + p.get_width() / 2., p.get_height()),
                        ha='center', va='bottom', fontsize=9, fontweight='bold')

    # Округляем значения на оси Y до целых
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))

    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.show()

    return ax


def freerisk_plot(weighted_YTM, weighted_maturity_date, currency):
    # Построение графика с эффективной доходностью портфеля относительно
    # безрисковой доходности

    df = get_riskoff_yeilds(currency)

    # Проверка существования данных по безрисковой ставке для нужной валюты
    if df is None or df.is_empty():
        print(f"\nНет данных для расчета безрисковой ставки по валюте {currency}!\n")
        return None

    # Основной график с настройками
    ax = sns.lineplot(data=df, x='period', y='value',
                      markers=True, linewidth=2, marker='o',
                      markersize=6, color='#3498DB',
                      markerfacecolor='white',  # Белая заливка маркеров
                      markeredgewidth=2, markeredgecolor='#3498DB',
                      label='Безрисковая доходность')

    # Добавляем специальную точку
    special_point = plt.scatter(x=weighted_YTM, y=weighted_maturity_date,
                                color='#E74C3C', s=100, zorder=5,
                                edgecolors='black', linewidth=2,
                                label='Портфель')

    # Настройки оформления
    title = f'Доходность {currency} портфеля на бизрисковой кривой'
    plt.title(title,
              fontsize=14, fontweight='bold', pad=25)
    plt.xlabel('Срок, лет', fontsize=14, labelpad=10)
    plt.ylabel('Эффективная доходность, %', fontsize=14, labelpad=10)

    # Улучшаем сетку и внешний вид
    plt.grid(True, alpha=0.4, linestyle='--')
    plt.legend(fontsize=12, framealpha=0.9)

    # Убираем лишние рамки
    sns.despine(left=True, bottom=True)

    plt.tight_layout()
    plt.show()