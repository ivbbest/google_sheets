from pprint import pprint

import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
from config import credentials_file, spreadsheet_id, DATABASE

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Date, select
from sqlalchemy.orm import sessionmaker
from psycopg2.errors import UniqueViolation
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func


import requests
from xml.etree import ElementTree

Base = declarative_base()

# Создаем объект Engine, который будет использоваться объектами ниже для связи с БД
engine = create_engine(URL.create(**DATABASE))
# Метод create_all создает таблицы в БД , определенные с помощью  DeclarativeBase
Base.metadata.create_all(engine)
# Создаем фабрику для создания экземпляров Session. Для создания фабрики в аргументе
# bind передаем объект engine
Session = sessionmaker(bind=engine)
# Создаем объект сессии из фабрики Session
session = Session()


class DataBaseSheet(Base):
    """
    Класс для создания модели sqlalchemy. Можно использовать разные СУБД.
    В данной задаче используется Postgres
    """
    __tablename__ = 'sheet'

    id = Column(Integer, nullable=False, unique=True, primary_key=True,
                autoincrement=True)
    id_number = Column(Integer, nullable=False, unique=True)
    order_number = Column(Integer, nullable=False, unique=True)
    price = Column(Integer, nullable=False)
    price_rub = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)

    def __str__(self):
        return f"{self.order_number}"

    @staticmethod
    def delete(orders):
        for elem in orders:
            session.query(DataBaseSheet).filter(DataBaseSheet.order_number == elem). \
                delete(synchronize_session=False)
            session.commit()


class GoogleSheetDate:
    """Класс для авторизации и чтения данных из Google Sheets"""

    def __init__(self, credentials_file, spreadsheet_id):
        self.credentials_file = credentials_file
        self.spreadsheet_id = spreadsheet_id

    def authorization(self):
        # Авторизуемся и получаем service — экземпляр доступа к API
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.credentials_file,
            ['https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive'])
        httpAuth = credentials.authorize(httplib2.Http())
        self.service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)
        self.drive = apiclient.discovery.build('drive', 'v3', http=httpAuth)

    def read_file(self):
        # Чтение файла
        self.authorization()
        values = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range='A:D',
            majorDimension='ROWS'
        ).execute()
        return values['values'][1:]

    def get_revisions_file(self):
        # Получение последней версии ревизии файла id
        self.authorization()
        values = self.drive.revisions().list(
            fileId=self.spreadsheet_id,
            fields='*',
            pageSize=1000
        ).execute()

        return values['revisions'][1]['id']


def main():
    gs = GoogleSheetDate(credentials_file, spreadsheet_id)
    google_file = gs.read_file()

    if len(diff_order_db_vs_sheet(google_file)) > 0:
        DataBaseSheet.delete(diff_order_db_vs_sheet(google_file))

    for line in google_file:
        id_number, order_number, price, date = line
        row = DataBaseSheet(id_number=id_number, order_number=order_number,
                            price=price, price_rub=convert_usd_to_rub(price),
                            date=date)

        # проверяем существует ли order_number, то есть такой элемент уже в базе
        if session.query(DataBaseSheet).filter_by(order_number=order_number).first() is not None:
            print(order_number)
            print('Есть такой элемент')

            session.query(DataBaseSheet).filter(DataBaseSheet.order_number == order_number).update(
                {
                    "id_number": id_number,
                    "order_number": order_number,
                    "price": price,
                    "price_rub": convert_usd_to_rub(price),
                    "date": date
                },
                synchronize_session=False
            )
            session.commit()

        else:
            # Если нет, то создаем новую запись.
            # row = DataBaseSheet(id_number=id_number, order_number=order_number,
            #                     price=price, price_rub=convert_usd_to_rub(price),
            #                     date=date)
            # ловим возможную ошибку, например, такая запись уже есть.
            # если ошибку нашли, то перехватываем и делаем rollback
            try:
                # Добавляем запись
                session.add(row)

                # добавляем данные в таблицу
                session.commit()
            except (UniqueViolation, IntegrityError) as e:
                print('A duplicate record already exists')
                session.rollback()
            finally:
                session.close()

            # А теперь попробуем вывести все посты , которые есть в нашей таблице
            # for row in session.query(DataBaseSheet):
            #     print(row)


def convert_usd_to_rub(cost_usd):
    """
    Функция для получения текущего курса доллара к рублю
    и конвертации cost_usd to rub.
    """
    url = 'https://www.cbr.ru/scripts/XML_daily.asp'
    res = requests.get(url).content
    exchange_rate = ElementTree.fromstring(res).findtext('.//Valute[@ID="R01235"]/Value')
    cost_in_rub = int(float(exchange_rate.replace(',', '.')) * float(cost_usd))

    return cost_in_rub


def diff_order_db_vs_sheet(order_sheet):
    """
     Анализ какие есть номера заказов в базе, а каких нет в гугл доке
     Если номеров заказов нет в гугл доке, то в дальнейшем удаляем из базы их.
    """
    set_order_db = set([elem[0] for elem in session.query(DataBaseSheet.order_number).all()])
    set_order_google = set([int(elem[1]) for elem in order_sheet])
    diff_order = list(set_order_db.difference(set_order_google))

    return diff_order


if __name__ == "__main__":
    main()
    # gs = GoogleSheetDate(credentials_file, spreadsheet_id)
    # pprint(gs.get_revisions_file())
    # breakpoint()

    """
    Осталось доделать:
    1. Удаление полей из БД, если их нет в Гугл доке.
    2. Добавить проверку ревизии гугл дока. Если она отличается от последней, до делать парсинг всего гугл дока и дальше
    разбор полетов: добавление, удаление, обновление строчек.
    3. Поставить пункт 2 на автомат при работе скрипта. WHile TRUE и sleep периодами, чтобы не упал скрипт.
    Посмотрел, какие ошибки могут возникать в случае работы WHile TRUE и как лучше подстраховать, чтобы 
    плюс минус в режиме онлайн все работало.
    4. Попробовать вынести в классы разные функции типа обновления/удаления.
    5. Сделать ветвления для удаления/обновления/добавления в main()
    6. Сделать проверку необязательных пунктов и постараться их допилить или хотя бы часть:
    
    
    а) Отправка в бот ТГ, если прошел срок https://flammlin.com/blog/2022/04/18/python-otpravka-soobshheniya-v-telegram/ или https://core.telegram.org/bots/api#available-methods или https://ru.stackoverflow.com/questions/931492/%D0%9E%D1%82%D0%BF%D1%80%D0%B0%D0%B2%D0%BA%D0%B0-%D1%81%D0%BE%D0%BE%D0%B1%D1%89%D0%B5%D0%BD%D0%B8%D1%8F-%D0%B2-%D0%BA%D0%B0%D0%BD%D0%B0%D0%BB-telegram-%D1%81%D1%80%D0%B5%D0%B4%D1%81%D1%82%D0%B2%D0%B0%D0%BC%D0%B8-python
    б) упаковка в докер контейнер https://dev.to/stefanopassador/docker-compose-with-python-and-posgresql-33kk 
    """
