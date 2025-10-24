import sqlite3
import re
import polars as pl


class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        return self.conn.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        self.conn.close()

    def is_date_string(self, value):
        """
        Проверяет, является ли строка датой в формате 'YYYY-MM-DD'
        """
        if not isinstance(value, str):
            return False

        # Проверка формата YYYY-MM-DD
        pattern = r'^\d{4}-\d{2}-\d{2}$'
        if re.match(pattern, value):
            return True

        return False

    def insert_dict(self, table_name, data_dict):
        with self as cursor:
            # Проверяем существование таблицы
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table_name,))

            table_exists = cursor.fetchone() is not None
            if not table_exists:
                # Создаем таблицу на основе ключей словаря
                columns = []
                for key, value in data_dict.items():
                    if isinstance(value, int):
                        col_type = 'INTEGER'
                    elif isinstance(value, float):
                        col_type = 'REAL'
                    elif self.is_date_string(value):
                        col_type = 'TEXT'
                    else:
                        col_type = 'TEXT'
                    columns.append(f'{key} {col_type}')

                create_query = f'''
                    CREATE TABLE {table_name} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        {", ".join(columns)}
                    )
                '''
                cursor.execute(create_query)

            columns = ', '.join(data_dict.keys())
            placeholders = ':' + ', :'.join(data_dict.keys())

            # Выполняем запрос - SQLite сам преобразует None в NULL
            cursor.execute(f'''
                INSERT INTO {table_name} ({columns})
                VALUES ({placeholders})
            ''', data_dict)

    def delete_table(self, table_name):
        "Удаление таблицы"
        with self as cursor:
            # Проверяем существование таблицы
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if cursor.fetchone():
                # Удаляем таблицу
                cursor.execute(f"DROP TABLE {table_name}")

    def fetch_data_from_sqlite(self, df, keys_list, table_name, join_parameter):
        """Получение данных об облигациях из SQLite по списку ISIN"""

        # Проверка что список ISIN не пустой
        if not keys_list:
            print("Передан пустой список ключей")
            return

        with self as cursor:
            placeholders = ','.join(['?'] * len(keys_list))

            query = f"""
            SELECT *
            FROM {table_name} 
            WHERE secid IN ({placeholders})
            """

            # Если self возвращает курсор, получаем соединение из него
            if hasattr(cursor, 'connection'):
                conn = cursor.connection
            else:
                # Или предполагаем, что cursor это соединение
                conn = cursor

            result_df = pl.read_database(query, conn, execute_options={'parameters': keys_list})

            result_df = df.join(result_df, on=join_parameter, how="inner")

            return result_df

    def currency_value(self, currency: str):
        # Получение знчаения валюты currency

        currency += 'FIX'

        # Для рублей возвращаем 1
        if currency == 'RUB' or currency == 'RUBFIX':
            return 1

        with self as cursor:
            query = """
            SELECT LASTVALUE
            FROM currency
            WHERE secid = ?
            """

            # Выполняем запрос с параметром
            cursor.execute(query, (currency,))

            # Получаем результат
            result = cursor.fetchone()

            return result