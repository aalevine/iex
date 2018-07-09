FROM python:2.7

ADD postgres.py
ADD iex.py
ADD sql/
ADD data/

RUN pip install psycopg2, requests

CMD [ "python", "./iex.py", "-b" ]
