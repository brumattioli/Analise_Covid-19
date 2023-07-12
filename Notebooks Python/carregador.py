# Pacotes necessários
import numpy as np
import pandas as pd
import psycopg2 
import pandas.io.sql as sqlio

# Conexão no postgre com o banco de dados Grupo_Beta
conn = psycopg2.connect("dbname=grupo_beta user=postgres password=vagan9ch")
cur = conn.cursor()

# Importação do conjunto de dados completo dos casos de COVID-19 no Brasil
covid_casos = pd.read_csv("G:/My Drive/Especialização/Disciplinas/Módulo 2/Linguagens de Programação para Ciência de Dados/Trabalho/Bases/caso_full.csv")

# Filtro com as cidades escolhidas
# Guarulhos, Atibaia, Osasco, Franco da Rocha e Bragança Paulista
covid_casos_filtro_cidade = covid_casos[(covid_casos['city'] == 'Atibaia') | (covid_casos['city'] == 'Osasco') | (covid_casos['city'] == 'Guarulhos') | (covid_casos['city'] == 'Bragança Paulista') | (covid_casos['city'] == 'Franco da Rocha')]

covid_casos_filtro = covid_casos_filtro_cidade[(covid_casos_filtro_cidade['is_repeated'] == False)]

#----------------------------------------------------------------------
# Imputando os dados da tabela / entidade semana epidemológica no BD
#----------------------------------------------------------------------

# Criação da tabela apenas com os dados necessários
semana_epidemologica = covid_casos_filtro[['epidemiological_week']]

# Retirada dos valores duplicados. Não é necessário ter os dados repetidos no BD.
semana_epidemologica2 = semana_epidemologica.drop_duplicates(subset = 'epidemiological_week')

# Criação de uma lista contendo os dados da coluna epidemiological_week
epid_week = semana_epidemologica2['epidemiological_week'].tolist()

# Criação de uma lista de dicionários contendo os dados da coluna epidemiological_week e
# com a chave semana_epid

# Esta lista de dicionários é necessária para poder executar o comando executemany do pacote psycopg2
semana_dicionario = {}
semana_lista = []

for i in range(0, len(epid_week)):
    
    semana_dicionario[i] = dict({'semana_epid':epid_week[i]})
    
    semana_lista.append(semana_dicionario[i])
    
# Imputação dos dados na tabela do BD semana_epidemologica
cur.executemany("""INSERT INTO semana_epidemologica (semana) VALUES (%(semana_epid)s);""", semana_lista)
    
conn.commit()

#----------------------------------------------------------------------
# Imputando os dados da tabela / entidade cidade no BD
#----------------------------------------------------------------------

# Criação da tabela apenas com os dados necessários
cidade = covid_casos_filtro[['city', 'city_ibge_code']]

# Retirada dos valores duplicados. Não é necessário ter os dados repetidos no BD.
cidade2 = cidade.drop_duplicates(subset = 'city')

# Criação de uma lista contendo os dados da coluna city
nome_cidade = cidade2['city'].tolist()

# Criação de uma lista contendo os dados da coluna city_ibge_code
cod_cidade = cidade2['city_ibge_code'].tolist()

# Criação de uma lista de dicionários contendo os dados da coluna epidemiological_week e
# com a chave semana_epid

# Esta lista de dicionários é necessária para poder executar o comando executemany do pacote psycopg2
cod_cidade_dicionario = {}
cod_cidade_lista = []

for i in range(0, len(cod_cidade)):
    
    cod_cidade_dicionario[i] = dict({'cod_cidade':cod_cidade[i],'nm_cidade':nome_cidade[i]})
    
    cod_cidade_lista.append(cod_cidade_dicionario[i])
    
# Imputação dos dados na tabela do BD cidade  
cur.executemany("""INSERT INTO cidade (nome_cidade, cod_cidade_ibge) VALUES (%(nm_cidade)s, (%(cod_cidade)s));""", cod_cidade_lista)
    
conn.commit()

#----------------------------------------------------------------------
# Imputando os dados da tabela / entidade data dado no BD
#----------------------------------------------------------------------

# Criação da tabela apenas com os dados necessários
data_dados = covid_casos_filtro[['last_available_date', 'epidemiological_week']]

# Retirada dos valores duplicados. Não é necessário ter os dados repetidos no BD.
data_dados2 = data_dados.drop_duplicates(subset = 'last_available_date')

# Recuperação do conteúdo da base semana_epidemologica que foi imputada anteriormente no BD
# Ela será utilizada para a captura da chave estrangeira id_sem_epid
sql_sem_epid = "SELECT * FROM semana_epidemologica;"
semana_epidemologica = sqlio.read_sql_query(sql_sem_epid, conn)

# Cruzamento entre a base data_dados2 e a base semana_epidemologica para a captura da chave estrangeira id_sem_epid
data_dados3 = pd.merge(data_dados2, semana_epidemologica, right_on='semana', left_on='epidemiological_week', how='left')

# Criação de uma lista contendo os dados da last_available_date
last_available_date = data_dados3['last_available_date'].tolist()

# Criação de uma lista contendo os dados da coluna id_sem_epid
id_sem_epid = data_dados3['id_sem_epid'].tolist()

# Esta lista de dicionários é necessária para poder executar o comando executemany do pacote psycopg2
data_dados_dicionario = {}
data_dados_lista = []

for i in range(0, len(last_available_date)):
    
    data_dados_dicionario[i] = dict({'data_dado':last_available_date[i],'id_sem_epid':id_sem_epid[i]})
    
    data_dados_lista.append(data_dados_dicionario[i])

# Imputação dos dados na tabela do BD data_dado  
cur.executemany("""INSERT INTO data_dado (data_dado, id_sem_epid) VALUES (%(data_dado)s, (%(id_sem_epid)s));""", data_dados_lista)
    
conn.commit()

#----------------------------------------------------------------------
# Imputando os dados da tabela / entidade população no BD
#----------------------------------------------------------------------

# Criação da tabela apenas com os dados necessários
populacao = covid_casos_filtro[['estimated_population_2019', 'estimated_population', 'city']]

# Retirada dos valores duplicados. Não é necessário ter os dados repetidos no BD.
populacao2 = populacao.drop_duplicates(subset = 'city')

# Recuperação do conteúdo da base cidade que foi imputada anteriormente no BD
# Ela será utilizada para a captura da chave estrangeira id_cidade
sql_cidade = "SELECT * FROM cidade;"
cidade = sqlio.read_sql_query(sql_cidade, conn)

# Cruzamento entre a base populacao2 e a base cidade para a captura da chave estrangeira id_cidade
populacao3 = pd.merge(populacao2, cidade, right_on='nome_cidade', left_on='city', how='left')

# Criação de uma lista contendo os dados da estimated_population_2019
estimated_population_2019 = populacao3['estimated_population_2019'].tolist()

# Criação de uma lista contendo os dados da estimated_population
estimated_population = populacao3['estimated_population'].tolist()

# Criação de uma lista contendo os dados da id_cidade
id_cidade = populacao3['id_cidade'].tolist()

# Esta lista de dicionários é necessária para poder executar o comando executemany do pacote psycopg2
populacao_dicionario = {}
populacao_lista = []

for i in range(0, len(id_cidade)):
    
    populacao_dicionario[i] = dict({'estimated_population_2019':estimated_population_2019[i],'estimated_population':estimated_population[i], 'id_cidade':id_cidade[i]})
    
    populacao_lista.append(populacao_dicionario[i])
    
# Imputação dos dados na tabela do BD populacao 
cur.executemany("""INSERT INTO populacao (pop_estimada_2019, pop_estimada, id_cidade) VALUES (%(estimated_population_2019)s, (%(estimated_population)s), (%(id_cidade)s));""", populacao_lista)
    
conn.commit()

#----------------------------------------------------------------------
# Imputando os dados da tabela / entidade casos no BD
#----------------------------------------------------------------------

# Criação da tabela apenas com os dados necessários
casos = covid_casos_filtro[['new_confirmed', 'city', 'last_available_date']]

# Recuperação do conteúdo da base cidade que foi imputada anteriormente no BD
# Ela será utilizada para a captura da chave estrangeira id_cidade
sql_cidade = "SELECT * FROM cidade;"
cidade = sqlio.read_sql_query(sql_cidade, conn)

# Recuperação do conteúdo da base data_dado que foi imputada anteriormente no BD
# Ela será utilizada para a captura da chave estrangeira id_data_dado
sql_data_dado = "SELECT * FROM data_dado;"
data_dado = sqlio.read_sql_query(sql_data_dado, conn)

# Cruzamento entre a base casos e a base cidade para a captura da chave estrangeira id_cidade
casos2 = pd.merge(casos, cidade, right_on='nome_cidade', left_on='city', how='left')

# Tratamento do campo de data para padronizar as duas chaves para o segundo cruzamento
casos2.last_available_date = pd.to_datetime(casos2.last_available_date)
data_dado.data_dado = pd.to_datetime(data_dado.data_dado)

# Cruzamento entre a base casos2 e a base data_dado para a captura da chave estrangeira id_data_dado
casos3 = pd.merge(casos2, data_dado, right_on='data_dado', left_on='last_available_date', how='left')

# Criação de uma lista contendo os dados da new_confirmed
new_confirmed = casos3['new_confirmed'].tolist()

# Criação de uma lista contendo os dados da id_cidade
id_cidade = casos3['id_cidade'].tolist()

# Criação de uma lista contendo os dados da id_data_dado
id_data_dado = casos3['id_data_dado'].tolist()

# Esta lista de dicionários é necessária para poder executar o comando executemany do pacote psycopg2
casos_dicionario = {}
casos_lista = []

for i in range(0, len(new_confirmed)):
    
    casos_dicionario[i] = dict({'new_confirmed':new_confirmed[i],'id_cidade':id_cidade[i], 'id_data_dado':id_data_dado[i]})
    
    casos_lista.append(casos_dicionario[i])
    

# Imputação dos dados na tabela do BD casos     
cur.executemany("""INSERT INTO casos (novos_casos, id_cidade, id_data_dado) VALUES (%(new_confirmed)s, (%(id_cidade)s), (%(id_data_dado)s));""", casos_lista)
    
conn.commit()

#----------------------------------------------------------------------
# Imputando os dados da tabela / entidade óbitos no BD
#----------------------------------------------------------------------

# Criação da tabela apenas com os dados necessários
obitos = covid_casos_filtro[['new_deaths', 'city', 'last_available_date']]

# Recuperação do conteúdo da base cidade que foi imputada anteriormente no BD
# Ela será utilizada para a captura da chave estrangeira id_cidade
sql_cidade = "SELECT * FROM cidade;"
cidade = sqlio.read_sql_query(sql_cidade, conn)

# Recuperação do conteúdo da base data_dado que foi imputada anteriormente no BD
# Ela será utilizada para a captura da chave estrangeira id_data_dado
sql_data_dado = "SELECT * FROM data_dado;"
data_dado = sqlio.read_sql_query(sql_data_dado, conn)

# Cruzamento entre a base obitos e a base cidade para a captura da chave estrangeira id_cidade
obitos2 = pd.merge(obitos, cidade, right_on='nome_cidade', left_on='city', how='left')

# Tratamento do campo de data para padronizar as duas chaves para o segundo cruzamento
obitos2.last_available_date = pd.to_datetime(obitos2.last_available_date)
data_dado.data_dado = pd.to_datetime(data_dado.data_dado)

# Cruzamento entre a base obitos2 e a base data_dado para a captura da chave estrangeira id_data_dado
obitos3 = pd.merge(obitos2, data_dado, right_on='data_dado', left_on='last_available_date', how='left')

# Criação de uma lista contendo os dados da new_deaths
new_deaths = obitos3['new_deaths'].tolist()

# Criação de uma lista contendo os dados da id_cidade
id_cidade = obitos3['id_cidade'].tolist()

# Criação de uma lista contendo os dados da id_data_dado
id_data_dado = obitos3['id_data_dado'].tolist()

# Esta lista de dicionários é necessária para poder executar o comando executemany do pacote psycopg2
obitos_dicionario = {}
obitos_lista = []

for i in range(0, len(new_deaths)):
    
    obitos_dicionario[i] = dict({'new_deaths':new_deaths[i],'id_cidade':id_cidade[i], 'id_data_dado':id_data_dado[i]})
    
    obitos_lista.append(obitos_dicionario[i])
   
# Imputação dos dados na tabela do BD obitos 
cur.executemany("""INSERT INTO obitos (novos_obitos, id_cidade, id_data_dado) VALUES (%(new_deaths)s, (%(id_cidade)s), (%(id_data_dado)s));""", obitos_lista)
    
conn.commit()