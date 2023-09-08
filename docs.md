[EN-US]
# File Documentation

This documentation aims to explain the code in the files: config, main, and utils.

## utils.py
This file is used to store functions that will be used in main.py.

### def convert_value(value)
Function used to convert the values of fields between PostgreSQL and MongoDB data types, 
due to the different field instances.

### def search_all_postgres_tables(pg_connection)
Function responsible for fetching the names and the number of foreign keys present in all PostgreSQL tables.

### def fetch_table_structure(pg_connection, table)
Fetches the columns and their data types in PostgreSQL for a given table.

### def fetch_reference_counts(pg_connection, table)
Fetches the number of tables that reference the table passed as a parameter.

### def check_fk_column(pg_connection, table, column)
Checks if the given column and table are a Foreign Key to another table.

### def fetch_pk_column(pg_connection, table)
Fetches the column that serves as the Primary Key in the table passed as a parameter.

## config.py
This file is used to manage the general configurations available for the import process.
Only the data in this file needs to be changed for the migration to take place.

### PG_CONFIG
Variable responsible for storing the connection data for PostgreSQL.

### MONGO_CONFIG
Variable responsible for storing the connection data for MongoDB.

### INSERT_OBJECT_ID_REFERENCES
Inserts the objectId in Foreign Key fields that have an ID.

### INSERT_NULL_FIELDS
Inserts fields that are NULL in PostgreSQL. The idea of MongoDB is to only have meaningful fields due to the absence of a fixed schema.

## main.py
This file is responsible for performing the migration from PostgreSQL to MongoDB.

**NO** changes are needed in this file to perform the migration unless there is a need for code correction.

### Aggregation Rules

- A table will only be aggregated if it is referenced by only one table.
- If the table is referenced by more than one table, you can insert the ObjectId for reference. In this case, check if the INSERT_OBJECT_ID_REFERENCES parameter in the config.py file is active.

### Reading the Code
```pycon
# Buscar tabelas do PostgreSQL
resultados = busca_todas_tabelas_postgress(pg_connection)

table_info = [(table[0], table[1]) for table in resultados]
```

Searches all tables in the PostgreSQL database and stores them in the table_info variable, where table[0]
is the name of the column and table[1] is the number of foreign keys in the table.

```pycon
for table, num_foreign_keys in table_info:
    if table in tabelas_verificadas:
        continue

    columns = busca_estrutura_tabela(pg_connection, table)
```
It goes through all the tables in the PostgreSQL base, if it is contained in the verified_tables array it will only go to the next iteration, if it is not it will look for the table columns.

```pycon
pg_cursor = pg_connection.cursor()
pg_cursor.execute(f"SELECT * FROM {table};")
data = pg_cursor.fetchall()

num_references_keys = busca_quantidades_referencias(pg_connection, table)

if num_foreign_keys == 0 and num_references_keys == 1:
    continue
```
Searches for the data rows in the table, after which it checks how many foreign keys reference the table in question,
if the table has no foreign key and is only referenced by another table, this table will be used for aggregation, due to this it will pass to the next iteration.

```pycon
mongo_collection = mongo_db[table]
for row in data:
    document = {}
    for i, value in enumerate(row):
```
Created a mongo collection from PostgreSQL table
and each row of the table and each field of the row will be traversed.

```pycon
column_name = pg_cursor.description[i].name
data_type = columns.get(column_name)

    if num_foreign_keys == 0:
        if convert_value(value):
            document[column_name] = convert_value(value)
        else:
            if (value is None and INSERT_NULL_FIELDS) or value is not None:
                document[column_name] = value
```
Checks if the table's foreign key number is Zero (0),
whether the value is required to convert types between postgreSQL and MongoDB.
If not necessary, it will insert the value, however if the parameter
INSERT_NULL_FIELDS is set to False, fields with null records will not be inserted.

```pycon
elif num_foreign_keys > 0:
    fk_info = verifica_campo_fk(pg_connection, table, column_name)

    if fk_info and fk_info[0] != table:
```
If the number of foreign keys in the table is greater than Zero (0),
checks if the field you are browsing is a foreign key.
It also checks that the destination of the foreign key is not the current table.

```pycon
num_references_keys_tabela_referenciada = busca_quantidades_referencias(pg_connection, fk_info[0])
if num_references_keys_tabela_referenciada == 1:
    pg_cursor3 = pg_connection.cursor()
    pg_cursor3.execute(f"SELECT * FROM {fk_info[0]} WHERE {fk_info[1]} = {value};")
    foreign_object = pg_cursor3.fetchone()

    column_names = [desc[0] for desc in pg_cursor3.description]
```
Checks the number of tables that reference the target table
is one (1), if it is one, it will fetch the row from the destination table
that has the referenced value.

```pycon
if foreign_object:
    column_name = fk_info[0]
    result_dict = {col_name: col_value for col_name, col_value in zip(column_names, foreign_object)}

    fk_info_subtable = ''
    col_value_name = ''
    col_value_subtable = ''
```
If the foreign_object exists, it will store the result in the result_dict.

```pycon
for j, col_value in enumerate(foreign_object):
    col_name = column_names[j]  # Nome da coluna
    fk_info_subtable = verifica_campo_fk(pg_connection, fk_info[0], col_name)

    if fk_info_subtable:
        result_dict[fk_info_subtable[0]] = result_dict.pop(col_name)

        num_references_keys_referenced_table = busca_quantidades_referencias(pg_connection, fk_info_subtable[0])

        if num_references_keys_referenced_table == 1:
            pg_cursor10 = pg_connection.cursor()
            pg_cursor10.execute(f"SELECT * FROM {fk_info_subtable[0]} WHERE {fk_info_subtable[1]} = {col_value};")
            values = pg_cursor10.fetchone()
            column_names_referenced = [desc[0] for desc in pg_cursor10.description]
            col_value_subtable = {col_name: col_value for col_name, col_value in zip(column_names_referenced, values)}
            col_value_name = fk_info_subtable[0]
        
            for name, valor in enumerate(column_names_referenced):
                if name > 0:
                    is_pk = verifica_campo_fk(pg_connection, fk_info_subtable[0], valor)
        
                    if is_pk:
                        col_value_subtable[is_pk[0]] = col_value_subtable.pop(valor)
        
            if fk_info_subtable[0] not in tabelas_verificadas:
                tabelas_verificadas.append(fk_info_subtable[0])
```
Loops through the columns of the foreign_object and checks if the field is
a foreign key. If so, remove the column from the result_dict.

Fetch the number of tables that reference, if equal to one (1)
will be aggregated, in this way the object is fetched from the destination table.
If the target table of the aggregation is not in the array of already checked tables,
will be added to not be traversed later.

```pycon
if table not in tabelas_verificadas:
    tabelas_verificadas.append(table)

if fk_info[0] not in tabelas_verificadas:
    tabelas_verificadas.append(fk_info[0])
```
If the root table or referenced table is not in the
in the array of tables already checked, it will be added so as not to
be traversed later.

```pycon
for key, valor in result_dict.items():
    if convert_value(valor) and ((valor is None and INSERT_NULL_FIELDS) or valor is not None):
        result_dict[key] = convert_value(valor)
    elif valor is None and INSERT_NULL_FIELDS is False:
        result_dict.pop(key)
        document[column_name] = result_dict

        if col_value_name and col_value_subtable:
            if (col_value_subtable is None and INSERT_NULL_FIELDS) or col_value_subtable is not None:
                result_dict[col_value_name] = col_value_subtable
```
Check the values ​​present in result_dict and, if necessary,
carries out the necessary treatments, it will only be inserted
null values ​​if the INSERT_NULL_FIELDS parameter is True.

```pycon
else:
    document[fk_info[0]] = value
```
If it is referenced by more than one table, the Id will be left
to later be changed by the ObjectId if the parameter
INSERT_OBJECT_ID_REFERENCES is True.
```pycon
    if INSERT_OBJECT_ID_REFERENCES:
        collection_names = mongo_db.list_collection_names()

        # Imprimir o conteúdo de todos os documentos em cada coleção
        for collection_name in collection_names:
            collection = mongo_db[collection_name]
            documents = collection.find()
            for document in documents:
                for collumn, value in document.items():
                    update_dict = {}
```
From this part, the MongoDB ObjectIds are inserted
checking if the field is a Foreign Key in PostgreSQL.

```pycon
if isinstance(value, int):
    coluna_pk = busca_campo_pk(pg_connection, collumn)
    if coluna_pk is not None:
        fk_info = verifica_campo_fk(pg_connection, collection_name, coluna_pk[0])
        if fk_info and fk_info[0] != collection_name:
            referenced_collection = mongo_db[fk_info[0]]
            matching_document = referenced_collection.find_one({fk_info[1]: value})
            if matching_document:
                mongo_db[collection_name].update_one(document,  {"$set": {collumn: ObjectId(matching_document['_id'])}})
```
If the instance of the field value is an integer, it will be checked if the field
is a Foreign Key and if so, the ObjectId will be added to the field.

```pycon
if isinstance(value, dict):
    for key, val in value.items():
        if isinstance(val, int):
            coluna_pk = busca_campo_pk(pg_connection, key)
            if coluna_pk is not None:
                fk_info = verifica_campo_fk(pg_connection, collection_name, coluna_pk[0])
                if fk_info and fk_info[0] != collection_name:
                    referenced_collection = mongo_db[fk_info[0]]
                    matching_document = referenced_collection.find_one({fk_info[1]: val})
                    if matching_document:
                        update_dict[f"{collumn}.{key}"] = ObjectId(matching_document['_id'])
        if isinstance(val, dict) and collumn != key:
            for k, v in val.items():
                if isinstance(v, int):
                    coluna_pk = busca_campo_pk(pg_connection, k)
                    if coluna_pk is not None:
                        fk_info = verifica_campo_fk(pg_connection, key, coluna_pk[0])
                        if fk_info and fk_info[0] != key:
                            referenced_collection = mongo_db[fk_info[0]]
                            matching_document = referenced_collection.find_one({fk_info[1]: v})
                            if matching_document:
                                update_dict[f"{collumn}.{key}.{k}"] = ObjectId(matching_document['_id'])
        if update_dict:
            mongo_db[collection_name].update_one({"_id": document["_id"]}, {"$set": update_dict})
```
If the field instance is an object (dict), it will be traversed
the object to check if it has integer fields and check if the
same are Foreign Key. It will also check if there is another object
and if it has, it will scroll to check if it has fields that are
It is necessary to change the ObjectId.

[PT-BR]
# Documentação dos arquivos

Esta documentação tem como objetivo explicar o código dos arquivos: config, main e utils.

## utils.py
Arquivo usado para armazenar as funções que serão utilizados no main.py.

### def convert_value(value)
Função usada para converter os valores dos campos entre os tipos do PostgreSql e MongoDB, 
devido as diferentes instâncias de campos. 

### def busca_todas_tabelas_postgress(pg_connection)
Função responsável por buscar o nome e a quantidade de foreign key presente em todas as tabelas do 
PostgreSQL.

### def busca_estrutura_tabela(pg_connection, table)
Busca as colunas e tipos de cada coluna no PostgreSQL.

### def busca_quantidades_referencias(pg_connection, table)
Busca a quantidade de tabelas que referenciam a tabela passada por parâmetro pela função.

### def verifica_campo_fk(pg_connection, table, collumn)
Verifica se a coluna e tabela passada é uma Foreign Key para outra tabela.

### def busca_campo_pk(pg_connection, table)
Busca o campo que é Primary Key na tabela passada por parâmetro para a função.

## config.py
Arquivo usado para gerenciar as configurações gerais disponíveis para o processo de importação, 
somente é necessário alterar os dados deste arquivo para a migração ser realizada.

### PG_CONFIG
Variável responsável por armazenar os dados de conexão com o PostgreSQL.

### MONGO_CONFIG
Variável responsável por armazenar os dados de conexão com o MongoDB.

### INSERT_OBJECT_ID_REFERENCES
Insere o objectId nos campos Foreign Key que ficaram com ID.

### INSERT_NULL_FIELDS
Insere os campos que estiverem NULL no PostgreSQL. A idéia do MongoDB é somente ter campos significativos 
devido não ter schema fixo. 

## main.py
Arquivo responsável por realizar a migração do PostgreSQL para o MongoDB.

**NÃO** é necessário realizar altearções neste arquivo para realizar a migração, 
somente se for necessário realizar alguma correção no código.

### Regras de agregação

- Uma tabela somente será agregada se for referenciada por somente 1 tabela. 
- Se a tabela for referenciada por mais de uma tabela, poderá ser colocado o ObjectId para referenciar, neste caso verifique se está ativo o pârametro INSERT_OBJECT_ID_REFERENCES no arquivo config.py

### Lendo o código

```pycon
# Buscar tabelas do PostgreSQL
resultados = busca_todas_tabelas_postgress(pg_connection)

table_info = [(table[0], table[1]) for table in resultados]
```

Busca todas as tabelas do banco PostgreSQL e armazena  na variavel table_info, sendo que o table[0]
é o nome da coluna e table[1] a quantidade de foreign keys da tabela.

```pycon
for table, num_foreign_keys in table_info:
    if table in tabelas_verificadas:
        continue

    columns = busca_estrutura_tabela(pg_connection, table)
```
Percorre todas as tabelas da base PostgreSQL, se estiver contida no array de tabelas_verificadas somente passará a próxima iteração, caso não estiver irá buscar as colunas da tabela.

```pycon
pg_cursor = pg_connection.cursor()
pg_cursor.execute(f"SELECT * FROM {table};")
data = pg_cursor.fetchall()

num_references_keys = busca_quantidades_referencias(pg_connection, table)

if num_foreign_keys == 0 and num_references_keys == 1:
    continue
```
Busca as linhas de dados da tabela, após isso verifica quantas foreign key referenciam a tabela em questão,
se a tabela não tiver foreign key e somente for referenciado por uma outra tabela, esta tabela será usada para agregação, devido isso passará a próxima iteração

```pycon
mongo_collection = mongo_db[table]
for row in data:
    document = {}
    for i, value in enumerate(row):
```
Criada uma colecao do mongo a partir da tabela do PostgreSQL 
e será percorrido cada linha da tabela e cada campo da linha.

```pycon
column_name = pg_cursor.description[i].name
data_type = columns.get(column_name)

    if num_foreign_keys == 0:
        if convert_value(value):
            document[column_name] = convert_value(value)
        else:
            if (value is None and INSERT_NULL_FIELDS) or value is not None:
                document[column_name] = value
```
Verifica se o numero de foreign key da tabela é Zero (0), 
se o valor é necessário converter tipos entre postgreSQL e MongoDB.
Caso não seja necessário, irá inserir o valor, porém se o parâmetro
INSERT_NULL_FIELDS estiver como False, não serão inseridos campos com registros nulos.

```pycon
elif num_foreign_keys > 0:
    fk_info = verifica_campo_fk(pg_connection, table, column_name)

    if fk_info and fk_info[0] != table:
```
Se o numero de foreign key da tabela for maior que Zero (0), 
verifica se o campo em que está percorrendo é uma foreign key.
Verifica também se o destino da foreign key não é a tabela atual.

```pycon
num_references_keys_tabela_referenciada = busca_quantidades_referencias(pg_connection, fk_info[0])
if num_references_keys_tabela_referenciada == 1:
    pg_cursor3 = pg_connection.cursor()
    pg_cursor3.execute(f"SELECT * FROM {fk_info[0]} WHERE {fk_info[1]} = {value};")
    foreign_object = pg_cursor3.fetchone()

    column_names = [desc[0] for desc in pg_cursor3.description]
```
Verifica o numero de tabelas que referenciam a tabela destino
é um (1), caso seja um, irá buscar a linha na tabela destino
que tem o valor referenciado.

```pycon
if foreign_object:
    column_name = fk_info[0]
    result_dict = {col_name: col_value for col_name, col_value in zip(column_names, foreign_object)}

    fk_info_subtable = ''
    col_value_name = ''
    col_value_subtable = ''
```
Se existir o foreign_object, irá armazenar o resultado no result_dict.

```pycon
for j, col_value in enumerate(foreign_object):
    col_name = column_names[j]  # Nome da coluna
    fk_info_subtable = verifica_campo_fk(pg_connection, fk_info[0], col_name)

    if fk_info_subtable:
        result_dict[fk_info_subtable[0]] = result_dict.pop(col_name)

        num_references_keys_referenced_table = busca_quantidades_referencias(pg_connection, fk_info_subtable[0])

        if num_references_keys_referenced_table == 1:
            pg_cursor10 = pg_connection.cursor()
            pg_cursor10.execute(f"SELECT * FROM {fk_info_subtable[0]} WHERE {fk_info_subtable[1]} = {col_value};")
            values = pg_cursor10.fetchone()
            column_names_referenced = [desc[0] for desc in pg_cursor10.description]
            col_value_subtable = {col_name: col_value for col_name, col_value in zip(column_names_referenced, values)}
            col_value_name = fk_info_subtable[0]
        
            for name, valor in enumerate(column_names_referenced):
                if name > 0:
                    is_pk = verifica_campo_fk(pg_connection, fk_info_subtable[0], valor)
        
                    if is_pk:
                        col_value_subtable[is_pk[0]] = col_value_subtable.pop(valor)
        
            if fk_info_subtable[0] not in tabelas_verificadas:
                tabelas_verificadas.append(fk_info_subtable[0])
```
Percorre as colunas do foreign_object e verifica se o campo é
uma foreign key. Se tiver, retira a coluna do result_dict.

Busca a quantidade de tabelas que referenciam, se for igual a um (1)
será agregado, desta forma é buscado o objeto na tabela destino.
Se a tabela destino da agregação não estiver no array de tabelas já verificadas,
será adicionada para não ser percorrida posteriormente.

```pycon
if table not in tabelas_verificadas:
    tabelas_verificadas.append(table)

if fk_info[0] not in tabelas_verificadas:
    tabelas_verificadas.append(fk_info[0])
```
Se a tabela raiz ou a tabela referenciada não estiverem na
no array de tabelas já verificadas, será adicionada para não 
ser percorrida posteriormente.

```pycon
for key, valor in result_dict.items():
    if convert_value(valor) and ((valor is None and INSERT_NULL_FIELDS) or valor is not None):
        result_dict[key] = convert_value(valor)
    elif valor is None and INSERT_NULL_FIELDS is False:
        result_dict.pop(key)
        document[column_name] = result_dict

        if col_value_name and col_value_subtable:
            if (col_value_subtable is None and INSERT_NULL_FIELDS) or col_value_subtable is not None:
                result_dict[col_value_name] = col_value_subtable
```
Verifica os valores presentes no result_dict e caso necessário 
realiza os tratamentos necessários, somente será inserido 
valores null se o parâmetro INSERT_NULL_FIELDS estiver como True.

```pycon
else:
    document[fk_info[0]] = value
```
Se for referenciado por mais de uma tabela, será deixado o Id
para posteriormente ser trocado pelo ObjectId caso o parâmetro
INSERT_OBJECT_ID_REFERENCES estiver True.

```pycon
    if INSERT_OBJECT_ID_REFERENCES:
        collection_names = mongo_db.list_collection_names()

        # Imprimir o conteúdo de todos os documentos em cada coleção
        for collection_name in collection_names:
            collection = mongo_db[collection_name]
            documents = collection.find()
            for document in documents:
                for collumn, value in document.items():
                    update_dict = {}
```
Apartir desta parte é realizado a inserção dos ObjectId do MongoDB 
verificando se o campo é uma Foreign Key no PostgreSQL.

```pycon
if isinstance(value, int):
    coluna_pk = busca_campo_pk(pg_connection, collumn)
    if coluna_pk is not None:
        fk_info = verifica_campo_fk(pg_connection, collection_name, coluna_pk[0])
        if fk_info and fk_info[0] != collection_name:
            referenced_collection = mongo_db[fk_info[0]]
            matching_document = referenced_collection.find_one({fk_info[1]: value})
            if matching_document:
                mongo_db[collection_name].update_one(document,  {"$set": {collumn: ObjectId(matching_document['_id'])}})
```
Se a instancia do valor do campo for inteiro, será verificado se o campo 
é uma Foreign Key e caso seja será adicionado o ObjectId ao campo.

```pycon
if isinstance(value, dict):
    for key, val in value.items():
        if isinstance(val, int):
            coluna_pk = busca_campo_pk(pg_connection, key)
            if coluna_pk is not None:
                fk_info = verifica_campo_fk(pg_connection, collection_name, coluna_pk[0])
                if fk_info and fk_info[0] != collection_name:
                    referenced_collection = mongo_db[fk_info[0]]
                    matching_document = referenced_collection.find_one({fk_info[1]: val})
                    if matching_document:
                        update_dict[f"{collumn}.{key}"] = ObjectId(matching_document['_id'])
        if isinstance(val, dict) and collumn != key:
            for k, v in val.items():
                if isinstance(v, int):
                    coluna_pk = busca_campo_pk(pg_connection, k)
                    if coluna_pk is not None:
                        fk_info = verifica_campo_fk(pg_connection, key, coluna_pk[0])
                        if fk_info and fk_info[0] != key:
                            referenced_collection = mongo_db[fk_info[0]]
                            matching_document = referenced_collection.find_one({fk_info[1]: v})
                            if matching_document:
                                update_dict[f"{collumn}.{key}.{k}"] = ObjectId(matching_document['_id'])
        if update_dict:
            mongo_db[collection_name].update_one({"_id": document["_id"]}, {"$set": update_dict})
```
Se a instancia do campo for um objeto (dict), será percorrido
o objeto para verifica se tem campos inteiros e verificar se os
mesmos são Foreign Key. Verificará também se há outro objeto
e caso tiver irá percorrer para verificar se tem campos que seja
necessário realizar a troca para ObjectId.