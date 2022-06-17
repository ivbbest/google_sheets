# Реализовать в режиме онлайн парсинг данных Google Sheets по API и добавления/обновления/удаления в таблицу на Postgres

## Задачи

1. Получать данные с документа при помощи Google API, сделанного в [Google Sheets](https://docs.google.com/spreadsheets/d/1LTejK-Oo7L1bFreBIIcEZnF1W1RCC1s_jos3EuIP0jI/edit?usp=sharing) (необходимо копировать в свой Google аккаунт и выдать самому себе права).
2. Данные должны добавляться в БД, в том же виде, что и в файле –источнике, с добавлением колонки «стоимость в руб.»
    
    a. Необходимо создать DB самостоятельно, СУБД на основе PostgreSQL.
    
    b. Данные для перевода $ в рубли необходимо получать по курсу [ЦБ РФ](https://www.cbr.ru/development/SXML/).
    
3. Скрипт работает постоянно для обеспечения обновления данных в онлайн режиме (необходимо учитывать, что строки в Google Sheets таблицу могут удаляться, добавляться и изменяться).

4. Упаковка решения в docker контейнер
5. Разработка функционала проверки соблюдения «срока поставки» из таблицы. В случае, если срок прошел, скрипт отправляет уведомление в Telegram.

## Общая информация

Для запуска скрипта требуется добавить (без этого не будет работать):

1. credentials_file - это json файл из Google API.
2. Данные по DATABASE в виде словаря (как пример ниже):

```dict
DATABASE = {
    'drivername': drivername,
    'host': 'localhost',
    'port': '5432',
    'username': username,
    'password': password,
    'database': database
}
```

3. token_tg - это токен Телеграм для отправки сообщения, если дата поставки прошла. Получаете через бота @BotFather.
4. chat_id - это тоже для Телеграм. Если не знаете, то используйте бота в Телеграм @userinfobot или другой вариант.

### Использование Docker

### Установка Docker.
Установите Docker, используя инструкции с официального сайта:
- для [Windows и MacOS](https://www.docker.com/products/docker-desktop)
- для [Linux](https://docs.docker.com/engine/install/ubuntu/). Отдельно потребуется установть [Docker Compose](https://docs.docker.com/compose/install/)

## Запуск проекта через Docker.
Склонируйте репозиторий `git clone https://github.com/ivbbest/google_sheets.git` в текущую папку.

### Настройка проекта

Создайте `.env` файл в корне репозитория:

```
      - POSTGRES_DB=database
      - POSTGRES_USER=username
      - POSTGRES_PASSWORD=secret
```
Пример `.env`:

```
      - POSTGRES_DB=database
      - POSTGRES_USER=username
      - POSTGRES_PASSWORD=secret
```

Внесите при необходимости корректировки в переменные окружения.


### Сборка проека

В корне репозитория выполните команду:

```bash
docker-compose build
```

При первом запуске данный процесс может занять несколько минут.

### Запуск проекта

```bash
docker-compose up
```

### Остановка контейнеров

Для остановки контейнеров выполните команду:

```bash
docker-compose stop
```

### Для остановки с удалением контейнеров 

```bash
docker-compose down
```

### Вывести список контейнеров

```bash
docker-compose ps
```

### Вывести список образов

```bash
docker-compose images
```

## Запуск проекта без Docker
1. Склонируйте репозиторий `git clone https://github.com/ivbbest/google_sheets.git` в текущую папку.
2. В консоли перейти в папку app `cd app`.
3. Набрать команду `python3 google_sheets.py`.

