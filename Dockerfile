FROM python:2.7
COPY . /app
WORKDIR /app
RUN pip install --trusted-host pypi.python.org -r requirements.txt
ARG pg_database
ENV database=$pg_database
ARG pg_user
ENV user=$pg_user
ARG pg_password
ENV password=$pg_password
EXPOSE 5432
CMD ["python", "app.py"]