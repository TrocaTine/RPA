import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import requests

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
 
# Função para criptografar a senha usando a API fornecida
def get_encrypted_password(password):
    url = "https://api-spring-boot-trocatine.onrender.com/users/encrypt-password"
    headers = {"Content-Type": "application/json"}
    body = {"password": password}
    response = requests.post(url, json=body, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        if not data.get("error"):
            return data["data"]["password"]
    print("Erro ao criptografar senha:", response.status_code)
    return None

# Função para inserir informações na tabela de destino 'Users'
def inserir_info_tabela_destino_usuario(df_usuarios, df_adm, conn_params):
    conn = None
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        # Obter usuários administradores para definir o campo 'admin'
        ids_adm = set(df_adm["idusuario"].values)
        
        # Manter rastreamento dos apelidos para evitar duplicidade
        apelidos_existentes = set()
        
        for index, row in df_usuarios.iterrows():
            # Nome
            first_name = row['nome']
            last_name = row['sobrenome']
            nickname_base = f"{first_name}_{last_name}"
            nickname = nickname_base
            num = 1
            while nickname in apelidos_existentes:
                nickname = f"{nickname_base}{num}"
                num += 1
            apelidos_existentes.add(nickname)
            
            # Admin
            admin = row['id'] in ids_adm
            
            # Criptografar senha
            encrypted_password = get_encrypted_password(row['senha'])
            
            # Inserir dados na tabela Users
            query_users = (f"INSERT INTO users (first_name, last_name, email, cpf, birth_date, admin, nickname, password)"
                           f" VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
            cur.execute(query_users, (first_name, last_name, row['email'], row['cpf'], row['dt_nascimento'], admin, nickname, encrypted_password))
        
        conn.commit()
        print("Dados inseridos com sucesso na tabela Users")
        
    except Exception as e:
        print(f"Erro ao inserir dados na tabela Users: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

# Função para inserir informações na tabela de destino 'Phones'
def inserir_info_tabela_destino_phones(df_usuarios, conn_params):
    conn = None
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        for index, row in df_usuarios.iterrows():
            # Buscar o id_user do usuário recém-inserido com base no e-mail
            cur.execute("SELECT id_user FROM users WHERE email = %s", (row['email'],))
            user_result = cur.fetchone()

            # Verificar se o usuário existe antes de inserir o telefone
            if user_result:
                id_user = user_result[0]

                # Inserir número de telefone na tabela Phones
                query_phones = f"INSERT INTO phones (id_user, number) VALUES (%s, %s)"
                cur.execute(query_phones, (id_user, row['telefone']))
            else:
                print(f"Usuário com e-mail {row['email']} não encontrado na tabela Users. Telefone não será inserido.")

        conn.commit()
        print("Dados inseridos com sucesso na tabela Phones")
    
    except Exception as e:
        print(f"Erro ao inserir dados na tabela Phones: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

# Função para extrair, transformar e transferir informações dos usuários
def transferindo_info_usuario(parametros_extracao, parametros_destino):
    # Extrair dados das tabelas de origem
    df_usuarios = extraindo_info_tabela_origem("usuario", parametros_extracao)
    df_adm = extraindo_info_tabela_origem("adm", parametros_extracao)

    if df_usuarios is not None and df_adm is not None:
        inserir_info_tabela_destino_usuario(df_usuarios, df_adm, parametros_destino)
        inserir_info_tabela_destino_phones(df_usuarios, parametros_destino)
    else:
        print("Erro ao extrair dados das tabelas de origem")

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

tabela_category = {
    "categoria":"categories"
}

transferindo_info_usuario(parametros_extracao, parametros_destino)