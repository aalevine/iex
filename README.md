# IEX

Utilizes the `company` and `chart` endpoints to derive the following tables:
1. `company`
2. `prices`

Based on the SMA (45d) and LMA (180d) of each stock price, uses the Simple Golden Cross trading strategy to derive the `orders` table.

[IEX API documentation](https://iextrading.com/developer/docs/)

## Getting Started

How to get a copy of the IEX project up and running on your local machine.

### Prerequisites

+ Download PostgreSQL on your local machine (e.g. using [Postgres.app](https://postgresapp.com/)).

+ Create a new database (or use the default, `postgres`) and specify your username and password. Default username for `postgres` is `postgres`, with no password.

+ Save your Postgres database name, username, and password to your `~/.bash_profile`. If no password, omit the third line below.
    ```
    export pg_database='postgres'
    export pg_user='austin'
    export pg_password='password'    
    ```

+ Clone this repo, and create a virtualenv to download the necessary packages.
    ```
    cd path/to/repo/
    virtualenv env
    source env/bin/activate
    pip install psycopg2 requests
    deactivate
    ```

### Running the Program

Execute the `app.py` file. 
+ There is an optional flag `-b` to perform a backfill:
    + Note: Your **first** execution should always include `-b` so the program knows to first create the tables (`company`, `prices`, `orders`).
    + With this flag, the program will drop (if necessary), (re)create, and backfill all three tables for the past two years.    
    + Without this flag, the program will fetch stock data for the past month and append to **existing** tables.


First time example run:

```
source env/bin/activate
python app.py -b
```

### Ongoing Executions

Set a cron job to run the program, say, once daily:

```
env EDITOR=vim crontab -e   
```
Insert a new job, while making variables from your .bash_profile available to cron:

```
0 5 * * * . ~/.bash_profile; cd ~/path/to/repo/ && source env/bin/activate && python app.py
```
