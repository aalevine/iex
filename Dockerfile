FROM python:2.7-slim
COPY . /app
WORKDIR /app
RUN pip install --trusted-host pypi.python.org -r requirements.txt

ARG pg_database=postgres
ENV database=$pg_database
ARG pg_user=austin
ENV user=$pg_user
ARG pg_password=password
ENV password=$pg_password

EXPOSE 5432
CMD ["python", "app.py"]