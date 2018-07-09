FROM python:2.7
COPY . /iex
WORKDIR /iex
RUN pip install -r requirements.txt
CMD python ./iex.py
