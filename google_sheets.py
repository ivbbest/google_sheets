from pprint import pprint

import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
from config import credentials_file, spreadsheet_id, DATABASE

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Date
from sqlalchemy.orm import sessionmaker
from psycopg2.errors import UniqueViolation
from sqlalchemy.exc import IntegrityError

import requests
from xml.etree import ElementTree


Base = declarative_base()


class DataBaseSheet(Base):
    """
    Класс для создания модели sqlalchemy. Можно использовать разные СУБД.
    В данной задаче используется Postgres
    """
    __tablename__ = 'sheet'

    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    id_number = Column(Integer, nullable=False, unique=True)
    order_number = Column(Integer, nullable=False, unique=True)
    price = Column(Integer, nullable=False)
    price_rub = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)

    def __str__(self):
        return f"{self.order_number}"


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
        return values['values'][3]

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
    # Создаем объект Engine, который будет использоваться объектами ниже для связи с БД
    engine = create_engine(URL.create(**DATABASE))
    # Метод create_all создает таблицы в БД , определенные с помощью  DeclarativeBase
    Base.metadata.create_all(engine)
    # Создаем фабрику для создания экземпляров Session. Для создания фабрики в аргументе
    # bind передаем объект engine
    Session = sessionmaker(bind=engine)
    # Создаем объект сессии из фабрики Session
    session = Session()

    # Создаем объект GoogleSheetDate для получения доступа к гугл документу и чтению данных

    gs = GoogleSheetDate(credentials_file, spreadsheet_id)
    id_number, order_number, price, date = gs.read_file()

    # Создаем новую запись.
    row = DataBaseSheet(id_number=id_number, order_number=order_number,
                        price=price, price_rub=current_exchange_usd_to_rub(price),
                        date=date)
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
    for row in session.query(DataBaseSheet):
        print(row)


def current_exchange_usd_to_rub(cost_usd):
    """
    Функция для получения текущего курса доллара к рублю
    и конвертации cost_usd to rub.
    """
    url = 'https://www.cbr.ru/scripts/XML_daily.asp'
    res = requests.get(url).content
    exchange_rate = ElementTree.fromstring(res).findtext('.//Valute[@ID="R01235"]/Value')
    cost_in_rub = int(float(exchange_rate.replace(',', '.')) * float(cost_usd))

    return cost_in_rub


if __name__ == "__main__":
    # main()
    gs = GoogleSheetDate(credentials_file, spreadsheet_id)
    pprint(gs.get_revisions_file())
    breakpoint()