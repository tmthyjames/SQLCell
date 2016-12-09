import os

# default connection string info here
driver = 'postgresql'
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
host = 'localhost'
port = '5432'
# empty until set by user
default_db = 'postgres' 
