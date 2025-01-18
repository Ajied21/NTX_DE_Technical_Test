SELECT 'CREATE DATABASE Data_Transform'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'data_transform')\gexec