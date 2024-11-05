import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path='.env')
 
def extraindo_info_tabela_origem(table_name, conn_params):
   
    conn_string = f"postgresql://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}"
   
    try:
        engine = create_engine(conn_string)

        query = f"SELECT * FROM {table_name}"

        df = pd.read_sql(query, engine)

        return df
    
    except Exception as e:
        print(f"Erro ao extrair dados da tabela {table_name}: {e}")
        return None
 
def colunas_tabela_destino(table_name, conn_params):
    
    conn = None
    try:

        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        print(table_name)

        query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' and table_schema = 'public'"

        cur.execute(query)
        
        resultado = cur.fetchall()
        for i in cur.fetchall():
            if i == ('categories',):
                print(i)
        
        columns = []
        for i in resultado:
            columns.append(i[0])
        return columns
    except Exception as e:
        print(f"Erro ao buscar colunas da tabela {table_name}: {e}")
        return []
    finally:
        
        if conn:
            cur.close()
            conn.close()
 
def inserir_info_tabela_destino_category(df, table_name, conn_params, target_columns):
    conn = None
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        placeholders  = ', '.join(['%s'] * len(target_columns))

        colunas_str = ', '.join(target_columns)

        query = (f"INSERT INTO {table_name} ({colunas_str}) VALUES ({placeholders})")
 
        for _, row in df.iterrows(): 
            cur.execute(query, tuple(row))
 
        conn.commit()
        print(f"Dados inseridos com sucesso na tabela {table_name}")
    except Exception as e:
        print(f"Erro ao inserir dados na tabela {table_name}: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
 
def transferindo_info_category(tabelas_mapeadas, parametros_extracao, parametros_destino):

    for tabela_origem, tabela_destino in tabelas_mapeadas.items():

        print(f"Transferindo dados da tabela {tabela_origem} para {tabela_destino}...")
       
        df = extraindo_info_tabela_origem(tabela_origem, parametros_extracao)
        if df is not None and not df.empty:
            target_columns = colunas_tabela_destino(tabela_destino, parametros_destino)
            if target_columns:
                inserir_info_tabela_destino_category(df, tabela_destino, parametros_destino, target_columns)
            else:
                print(f"Erro ao buscar colunas da tabela de destino {tabela_destino}")
        else:
            print(f"Não há dados para transferir na tabela {tabela_origem}")
 
def inserir_info_tabela_destino_tag(df, table_name, conn_params, target_columns):
    conn = None
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        print(target_columns)
        target_columns= target_columns[1:]
        colunas_str = ', '.join(target_columns)
        
        cur.execute(f"SELECT DISTINCT name FROM {table_name}")
        valores_existentes = set(row[0] for row in cur.fetchall())  

        for i in df:
            if i != 'id' and i != 'idtipo_produto':
                
                valores_unicos = set(df[i].dropna())

                for j in valores_unicos:
                    if j not in valores_existentes:  
                        print(j)  
                        query = (f"INSERT INTO {table_name} ({colunas_str}) VALUES ('{i.capitalize()}','{j}')")
                        cur.execute(query)  

            else:
                pass
      
        conn.commit()
        print(f"Dados inseridos com sucesso na tabela {table_name}")

    except Exception as e:
        print(f"Erro ao inserir dados na tabela {table_name}: {e}")
    finally:
        if conn:
            cur.close()
            conn.close() 
 
def transferindo_info_tag(tabelas_mapeadas, parametros_extracao, parametros_destino):

    for tabela_origem, tabela_destino in tabelas_mapeadas.items():

        print(f"Transferindo dados da tabela {tabela_origem} para {tabela_destino}...")
       
        df = extraindo_info_tabela_origem(tabela_origem, parametros_extracao)
        if df is not None and not df.empty:
            target_columns = colunas_tabela_destino(tabela_destino, parametros_destino)
            if target_columns:
                inserir_info_tabela_destino_tag(df, tabela_destino, parametros_destino, target_columns)
            else:
                print(f"Erro ao buscar colunas da tabela de destino {tabela_destino}")
        else:
            print(f"Não há dados para transferir na tabela {tabela_origem}")

parametros_extracao = {
    "dbname": os.getenv("DB_NAME_PRIMEIRO_ANO"),
    "user": os.getenv("DB_USER"),
    "host": os.getenv("DB_HOST"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv('DB_PORT')
}
 
parametros_destino = {
    "dbname": os.getenv("DB_NAME_SEGUNDO_ANO"),
    "user": os.getenv("DB_USER"),
    "host": os.getenv("DB_HOST"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv('DB_PORT')
}
 
tabela_tag = {
    "tag": "tags"      
}

#categoria
tabela_category = {
    "categoria":"categories"
}

transferindo_info_tag(tabela_tag, parametros_extracao, parametros_destino)
transferindo_info_category(tabela_category,parametros_extracao,parametros_destino)
