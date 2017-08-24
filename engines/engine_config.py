import os

# default connection string info here
driver = 'postgresql'
username = os.getenv('DB_USERNAME') or 'undefined'
password = os.getenv('DB_PASSWORD') or 'undefined'
host = 'localhost'
port = '5432'
# empty until set by user
default_db = 'sports'
