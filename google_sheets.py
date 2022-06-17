from pprint import pprint

import httplib2
import time
import sys
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
from config import credentials_file, spreadsheet_id, DATABASE, revisions_version

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Date
from sqlalchemy.orm import sessionmaker

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
        """Удаление записи в БД"""
        for elem in orders:
            session.query(DataBaseSheet).filter(DataBaseSheet.order_number == elem). \
                delete(synchronize_session=False)
            session.commit()

    @staticmethod
    def update(data_order):
        """Обновление данных в БД"""
        id_number, order_number, price, date = data_order
        session.query(DataBaseSheet).filter(DataBaseSheet.order_number == order_number).update(
            {
                "id_number": id_number,
                "order_number": order_number,
                "price": price,
                "price_rub": GoogleSheetDate.convert_usd_to_rub(price),
                "date": date
            },
            synchronize_session=False
        )
        session.commit()

    @staticmethod
    def is_exist(order_number):
        """Проверка существует ли запись в базе"""
        exist = session.query(DataBaseSheet).filter_by(order_number=order_number). \
                    first() is not None

        return exist

    @staticmethod
    def is_changes(date_sheet):
        """Проверка на изменения данных в базе по конкретной заказу"""
        change = False
        id_number, order_number, price, date = date_sheet
        elem = session.query(DataBaseSheet).filter_by(order_number=order_number).first()

        if elem.id_number != id_number or elem.price != price or elem.date != date:
            change = True

        return change


class GoogleSheetDate:
    """Класс для авторизации и чтения данных из Google Sheets"""
    def __init__(self, credentials_file, spreadsheet_id):
        # self.drive = None
        # self.service = None
        self.credentials_file = credentials_file
        self.spreadsheet_id = spreadsheet_id

    def authorization(self):
        """Авторизуемся и получаем service — экземпляр доступа к API"""
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.credentials_file,
            ['https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive'])
        httpAuth = credentials.authorize(httplib2.Http())
        self.service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)
        self.drive = apiclient.discovery.build('drive', 'v3', http=httpAuth)

    def read_file(self):
        """Чтение файла"""
        self.authorization()
        values = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range='A:D',
            majorDimension='ROWS'
        ).execute()
        return values['values'][1:5]

    def check_revisions_sheet(self):
        """Получение последней версии ревизии файла id"""
        update = False
        self.authorization()
        values = self.drive.revisions().list(
            fileId=self.spreadsheet_id,
            fields='*',
            pageSize=1000
        ).execute()

        сurrent_revisions = values['revisions'][-1]['id']

        with open(revisions_version, 'r+') as f:
            try:
                prev_revisions = f.readline().strip()

                print(prev_revisions)
                print(сurrent_revisions)

                if int(сurrent_revisions) > int(prev_revisions):
                    print(prev_revisions)
                    update = True
            except ValueError:
                print('Некорректный prev_revisions. Проверьте данные.')
            finally:
                f.seek(0)
                f.write(сurrent_revisions)
                print(update)

        return update

    @staticmethod
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

    @staticmethod
    def diff_order_db_vs_sheet(order_sheet):
        """
         Анализ какие есть номера заказов в базе, а каких нет в гугл доке
         Если номеров заказов нет в гугл доке, то в дальнейшем удаляем из базы их.
        """
        set_order_db = set([elem[0] for elem in session.query(DataBaseSheet.order_number).all()])
        set_order_google = set([int(elem[1]) for elem in order_sheet])
        diff_order = list(set_order_db.difference(set_order_google))

        return diff_order


def main():
    print('Запустили main')
    gs = GoogleSheetDate(credentials_file, spreadsheet_id)
    # Запускаем цикл для проверки в режиме онлайн обновлений в Google Sheet
    while True:
        try:
            print('Попали в вечный цикл')
            # если изменилась версия Google Sheet, то парсим данные
            if gs.check_revisions_sheet():
                print('Ecть изменения, будем проверять.')
                time.sleep(2)
                google_file = gs.read_file()
                diff_order = gs.diff_order_db_vs_sheet(google_file)
                # если в бд есть заказы, которых нет в Sheet, то удаляем
                if len(diff_order):
                    print('Есть лишние товары в БД. Удалять надо.')
                    time.sleep(2)
                    DataBaseSheet.delete(diff_order)

                # считываем данные построчно
                for line in google_file:
                    id_number, order_number, price, date = line
                    row = DataBaseSheet(id_number=id_number, order_number=order_number,
                                        price=price, price_rub=gs.convert_usd_to_rub(price),
                                        date=date)

                    # проверяем существует ли order_number, то есть такой элемент уже в базе
                    if DataBaseSheet.is_exist(order_number):
                        print(order_number)
                        print('Есть такой элемент')
                        time.sleep(2)
                        if DataBaseSheet.is_changes(line):
                            print('Данные по заказу изменились. Надо обновлять')
                            time.sleep(2)
                            DataBaseSheet.update(line)
                    else:
                        # Если нет, то создаем новую запись.
                        # Добавляем запись
                        print('Новые товары. Надо добавлять в базу')
                        time.sleep(2)
                        session.add(row)
                        # добавляем данные в таблицу
                        session.commit()
        # Если возникает ошибка при длительном ожидание ответа, то перехватываем
        # Делаем sleep и обратно в работу
        except (requests.exceptions.ConnectionError, TimeoutError,
                requests.exceptions.Timeout,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.ReadTimeout):

            print("\n Переподключение к Google Sheet \n")
            time.sleep(5)

        # если нет обновлений, то sleep
        time.sleep(15)
        print('sleeeeeeeeep')


if __name__ == "__main__":
    main()
