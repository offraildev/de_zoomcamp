FROM python:3.9

RUN apt-get install wget

WORKDIR /app

COPY ingest_data.py ingest_data.py
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

ENTRYPOINT [ "python", "ingest_data.py" ]