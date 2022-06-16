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
from sqlalchemy.orm.attributes import InstrumentedAttribute

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

    # def update(self):
    #     s = Session()
    #     mapped_values = {}
    #     for item in DataBaseSheet.__dict__.items():
    #         field_name = item[0]
    #         field_type = item[1]
    #         is_column = isinstance(field_type, InstrumentedAttribute)
    #         if is_column:
    #             mapped_values[field_name] = getattr(self, field_name)
    #
    #     s.query(DataBaseSheet).filter(DataBaseSheet.id == self.id).update(mapped_values)
    #     s.commit()

    # def add(self):
    #     pass

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
        return values['values'][1:3]

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
    # Создаем объект сессии из фабрики Session
    # session = Session()
    # Создаем объект GoogleSheetDate для получения доступа к гугл документу и чтению данных
    # breakpoint()
    gs = GoogleSheetDate(credentials_file, spreadsheet_id)
    google_file = gs.read_file()
    for line in google_file:
        id_number, order_number, price, date = line
        row = DataBaseSheet(id_number=id_number, order_number=order_number,
                            price=price, price_rub=current_exchange_usd_to_rub(price),
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
                    "price_rub": current_exchange_usd_to_rub(price),
                    "date": date
                },
                synchronize_session=False
            )
            session.commit()
            # # breakpoint()
            # elem = session.query(DataBaseSheet).filter_by(order_number == order_number).first()
            # if elem.id_number != id_number or elem.price != price or elem.date != date:
            #     elem.update(
            #         {
            #             "id_number": id_number,
            #             "order_number": order_number,
            #             "price": price,
            #             "price_rub": current_exchange_usd_to_rub(price),
            #             "date": date
            #         },
            #         synchronize_session='evaluate'
            #     )
            #     # update_row(elem, line)
            #     session.commit()
            # if elem.id_number != id_number or elem.price != price or elem.date != date:
            #     elem.id_number = id_number
            #     elem.price = price
            #     elem.date = date
            #     elem.price_rub = current_exchange_usd_to_rub(price)
            #     session.commit()

            print('Есть такой элемент')
            # делаем обработку, есть ли изменения в данных и если они есть, то мы меняем данные
            # если нет, то переходим к другому элементы из гугл таблицы
        else:
            # Если нет, то создаем новую запись.
            # row = DataBaseSheet(id_number=id_number, order_number=order_number,
            #                     price=price, price_rub=current_exchange_usd_to_rub(price),
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


# def update_row(row, line):
#     id_number, order_number, price, date = line
#     if row.id_number != row or row.price != price or row.date != date:
#         row.id_number = id_number
#         row.price = price
#         row.date = date
#         row.price_rub = current_exchange_usd_to_rub(price)
#         # session.commit()

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
    main()
    # gs = GoogleSheetDate(credentials_file, spreadsheet_id)
    # pprint(gs.get_revisions_file())
    # breakpoint()
