# Pacotes necessários
import numpy as np
import pandas as pd
import psycopg2 
import pandas.io.sql as sqlio

# Conexão no postgre com o banco de dados Grupo_Beta
conn = psycopg2.connect("dbname=grupo_beta user=postgres password=vagan9ch")
cur = conn.cursor()

# Criação das tabelas modeladas no Postgre
cur.execute("CREATE TABLE semana_epidemologica (id_sem_epid SERIAL PRIMARY KEY, semana INTEGER);")
cur.execute("CREATE TABLE data_dado (id_data_dado SERIAL PRIMARY KEY, data_dado DATE, id_sem_epid INTEGER);")
cur.execute("CREATE TABLE casos (id_casos SERIAL PRIMARY KEY, novos_casos INTEGER, id_cidade INTEGER, id_data_dado INTEGER);")
cur.execute("CREATE TABLE obitos (id_obitos SERIAL PRIMARY KEY, novos_obitos INTEGER, id_cidade INTEGER, id_data_dado INTEGER);")
cur.execute("CREATE TABLE populacao (id_populacao SERIAL PRIMARY KEY, pop_estimada_2019 INTEGER, pop_estimada VARCHAR, id_cidade INTEGER);")
cur.execute("CREATE TABLE cidade (id_cidade SERIAL PRIMARY KEY, nome_cidade VARCHAR, cod_cidade_ibge INTEGER);")

# Atribuição das chaves estrangeiras
cur.execute("ALTER TABLE data_dado ADD FOREIGN KEY (id_sem_epid) REFERENCES semana_epidemologica (id_sem_epid);")
cur.execute("ALTER TABLE populacao ADD FOREIGN KEY (id_cidade) REFERENCES cidade (id_cidade);")
cur.execute("ALTER TABLE casos ADD FOREIGN KEY (id_cidade) REFERENCES cidade (id_cidade);")
cur.execute("ALTER TABLE casos ADD FOREIGN KEY (id_data_dado) REFERENCES data_dado (id_data_dado);")
cur.execute("ALTER TABLE obitos ADD FOREIGN KEY (id_cidade) REFERENCES cidade (id_cidade);")
cur.execute("ALTER TABLE obitos ADD FOREIGN KEY (id_data_dado) REFERENCES data_dado (id_data_dado);")

# Executar as alterações no BD
conn.commit()