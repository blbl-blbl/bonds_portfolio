from df_process import portfolio_upload
from currency import get_currency


def main(path="bonds.xlsx", update_currency=True):
    """

    :param path: путь к файлу
    :param update_currency: bool обновлять котировки по валютам
    :return:
    """
    if update_currency:
        get_currency()

    portfolio_upload(path=path)


if __name__ == '__main__':
    main(update_currency=False)

