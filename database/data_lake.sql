SELECT 'CREATE DATABASE Data_Lake'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'data_lake')\gexec