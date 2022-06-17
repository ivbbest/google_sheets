FROM python:latest
WORKDIR /code
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY google_sheets.py google_sheets.py
CMD ["python3", "google_sheets.py"]