# Overview

Este projeto contém um pipeline de extração, transformação e carga de dados (ETL) que conecta-se a um banco de dados PostgreSQL para transferir e inserir dados entre tabelas de duas bases diferentes. O código inclui funções para extrair dados de tabelas de origem, mapear e transformar as colunas conforme necessário e inserir os dados nas tabelas de destino.

# Requisitos
Para executar este código, é necessário ter instalado:

Python 3.7 ou superior
As bibliotecas listadas no arquivo requirements.txt (veja abaixo como instalar)
Um arquivo .env para armazenar variáveis sensíveis

# Dependências
Instale as dependências necessárias com o comando:
pip install -r requirements.txt

# Variáveis de ambiente
Crie um arquivo .env na raiz do projeto e configure as seguintes variáveis de ambiente:
```
DB_NAME_PRIMEIRO_ANO=<nome_do_banco_de_dados_origem>
DB_NAME_SEGUNDO_ANO=<nome_do_banco_de_dados_destino>
DB_USER=<usuario_do_banco>
DB_PASSWORD=<senha_do_banco>
DB_HOST=<endereco_do_host>
DB_PORT=<porta_do_banco>
```

# Estrutura do Código
O código está dividido em várias funções principais para facilitar a modularidade e a manutenção.

## Funções Principais
*extraindo_info_tabela_origem*: Conecta-se ao banco de dados e extrai todas as colunas da tabela especificada.

*colunas_tabela_destino*: Retorna a lista de colunas da tabela de destino, usada para garantir que as inserções sejam compatíveis com a estrutura.

*get_encrypted_password*: Faz uma requisição HTTP para uma API externa para criptografar a senha dos usuários antes de inseri-la no banco de dados.

*inserir_info_tabela_destino_usuario*: Insere os dados de usuários na tabela de destino, incluindo validações para evitar duplicatas de apelidos e e-mails.

*inserir_info_tabela_destino_phones*: Insere os números de telefone dos usuários com base no ID correspondente.

*transferindo_info_usuario*: Organiza o fluxo de extração, transformação e carga dos dados de usuários e seus telefones.

*inserir_info_tabela_destino_category*: Insere dados na tabela de categorias, validando duplicação de nomes.

*transferindo_info_category*: Organiza a transferência de informações de categorias, extraindo da tabela de origem e inserindo na tabela de destino.

*inserir_info_tabela_destino_tag*: Insere tags únicas na tabela de destino, verificando duplicação de nomes.

*transferindo_info_tag*: Organiza a transferência de informações das tags.

## Estrutura de Chamada
As funções transferindo_info_tag, transferindo_info_category e transferindo_info_usuario são chamadas ao final do script para iniciar o processo de ETL para cada conjunto de dados.

## Executando o Código
Execute o código principal para iniciar o processo de transferência de dados. O código realizará as operações de ETL entre as tabelas especificadas nas bases de origem e destino.

Copiar código:
python script.py

## Exemplo de Mapeamento de Tabelas
O mapeamento entre tabelas de origem e destino é realizado através de dicionários. Por exemplo:

Copiar código:

tabela_tag = {
    "tag": "tags"
}

tabela_category = {
    "categoria": "categories"
}

Esses mapeamentos são usados pelas funções de transferência para saber quais tabelas de destino correspondem às tabelas de origem.

## Observações
Este script é configurado para uso com bancos de dados PostgreSQL.
Certifique-se de que as tabelas de destino tenham a estrutura adequada antes de executar o código para evitar erros de inserção.
