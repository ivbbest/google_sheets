from pprint import pprint

import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
from config import credentials_file, spreadsheet_id, DATABASE


from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.orm import sessionmaker



Base = declarative_base()



class Sheet(Base):
    __tablename__ = 'sheet'

    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    id_number = Column(Integer, nullable=False)
    order_number = Column(Integer, nullable=False)
    price = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)

    def __str__(self):
        return f"{self.order_number}"



def main():
    #Создаем объект Engine, который будет использоваться объектами ниже для связи с БД
    engine = create_engine(URL.create(**DATABASE))
    #Метод create_all создает таблицы в БД , определенные с помощью  DeclarativeBase
    Base.metadata.create_all(engine)
    # Создаем фабрику для создания экземпляров Session. Для создания фабрики в аргументе
    # bind передаем объект engine
    Session = sessionmaker(bind=engine)
    # Создаем объект сессии из вышесозданной фабрики Session
    session = Session()

    # Создаем новую запись.
    new_post = Sheet(id_number='133', order_number="1313", price='13', date='13.05.2022')

    # Добавляем запись
    session.add(new_post)

    # Благодаря этой строчке мы добавляем данные а таблицу
    session.commit()

    # А теперь попробуем вывести все посты , которые есть в нашей таблице
    for post in session.query(Sheet):
        print(post)

# CREDENTIALS_FILE = credentials_file  # Имя файла с закрытым ключом, вы должны подставить свое
#
# # ID Google Sheets документа (можно взять из его URL)
# spreadsheet_id = spreadsheet_id
#
# # Авторизуемся и получаем service — экземпляр доступа к API
# credentials = ServiceAccountCredentials.from_json_keyfile_name(
#     CREDENTIALS_FILE,
#     ['https://www.googleapis.com/auth/spreadsheets',
#      'https://www.googleapis.com/auth/drive'])
# httpAuth = credentials.authorize(httplib2.Http())
# service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)
#
# # Пример чтения файла
# values = service.spreadsheets().values().get(
#     spreadsheetId=spreadsheet_id,
#     range='A:D',
#     majorDimension='ROWS'
# ).execute()
# breakpoint()
# pprint(values)

if __name__ == "__main__":
    main()