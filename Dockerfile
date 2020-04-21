FROM python:3.7.5-slim

RUN mkdir /app

COPY ./app /app/

WORKDIR app

RUN mkdir ./campaigns_activities

RUN pip install -r requirements.txt

CMD python add_app_directory_to_pythonpath.py

CMD python etl.py


