import psycopg2
from bson import BSON
from bson.objectid import ObjectId
from pymongo import MongoClient
import traceback
from config import PG_CONFIG, MONGO_CONFIG
from utils import convert_value, busca_todas_tabelas_postgress, busca_estrutura_tabela, busca_quantidades_referencias, verifica_campo_pk

# Conexão com o banco de dados MongoDB
mongo_client = MongoClient(MONGO_CONFIG['host'], MONGO_CONFIG['port'])
mongo_db = mongo_client[MONGO_CONFIG['dbname']]

# Conexão com o banco de dados PostgreSQL
pg_connection = psycopg2.connect(**PG_CONFIG)

try:
    tabelas_verificadas = list()
    tabelas_verificar_mongo = list()

    # Buscar tabelas do PostgreSQL
    resultados = busca_todas_tabelas_postgress(pg_connection)

    table_info = [(table[0], table[1]) for table in resultados]

    for table, num_foreign_keys in table_info:
        if table in tabelas_verificadas:
            continue

        # Buscar estrutura da tabela
        columns = busca_estrutura_tabela(pg_connection, table)

        # Buscar dados da tabela
        pg_cursor = pg_connection.cursor()
        pg_cursor.execute(f"SELECT * FROM {table};")
        data = pg_cursor.fetchall()

        # Quantidade de tabelas que referenciam a tabela atual (Tabela A)
        num_references_keys = busca_quantidades_referencias(pg_connection, table)

        if num_foreign_keys == 0 and num_references_keys == 1:
            continue

        # Inserir dados no MongoDB
        mongo_collection = mongo_db[table]
        for row in data:
            document = {}
            for i, value in enumerate(row):
                # print(table, num_foreign_keys, num_references_keys)
                column_name = pg_cursor.description[i].name
                data_type = columns.get(column_name)

                if num_foreign_keys == 0:
                    if convert_value(value):
                        document[column_name] = convert_value(value)
                    else:
                        document[column_name] = value

                elif num_foreign_keys > 0:
                    fk_info = verifica_campo_pk(pg_connection, table, column_name)

                    if fk_info and fk_info[0] != table:
                        # Quantidade de tabelas que referenciam a tabela referenciada (Tabela B)
                        num_references_keys_tabela_referenciada = busca_quantidades_referencias(pg_connection, fk_info[0])
                        if num_references_keys_tabela_referenciada == 1:
                            pg_cursor3 = pg_connection.cursor()
                            pg_cursor3.execute(f"SELECT * FROM {fk_info[0]} WHERE {fk_info[1]} = {value};")
                            foreign_object = pg_cursor3.fetchone()

                            column_names = [desc[0] for desc in pg_cursor3.description]
                            if foreign_object:
                                result_dict = {col_name: col_value for col_name, col_value in zip(column_names, foreign_object)}
                                fk_info_subtable = ''
                                col_value_name = ''
                                col_value_subtable = ''
                                for j, col_value in enumerate(foreign_object):
                                    col_name = column_names[j]  # Nome da coluna
                                    fk_info_subtable = verifica_campo_pk(pg_connection, fk_info[0], col_name)

                                    if fk_info_subtable:
                                        # Verificar se a subtabela (Tabela C) que 
                                        # está contida em Tabela B somente tem uma referencia
                                        num_references_keys_referenced_table = busca_quantidades_referencias(pg_connection, fk_info_subtable[0])

                                        if num_references_keys_referenced_table == 1:
                                            pg_cursor10 = pg_connection.cursor()
                                            pg_cursor10.execute(f"SELECT * FROM {fk_info_subtable[0]} WHERE {fk_info_subtable[1]} = {col_value};")
                                            values = pg_cursor10.fetchone()
                                            column_names_referenced = [desc[0] for desc in pg_cursor10.description]
                                            col_value_subtable = {col_name: col_value for col_name, col_value in zip(column_names_referenced, values)}
                                            col_value_name = col_name
                                            if fk_info_subtable[0] not in tabelas_verificadas:
                                                tabelas_verificadas.append(fk_info_subtable[0])
                                                print(tabelas_verificadas, fk_info_subtable[0], "abc")
                                if table not in tabelas_verificadas:
                                    tabelas_verificadas.append(table)
                                    print(tabelas_verificadas, table, "def")

                                if fk_info[0] not in tabelas_verificadas:
                                    tabelas_verificadas.append(fk_info[0])
                                    print(tabelas_verificadas, table, fk_info[0], "fgh")

                                for key, valor in result_dict.items():
                                    if convert_value(valor):
                                        result_dict[key] = convert_value(valor)
                                document[column_name] = result_dict

                                if col_value_name and col_value_subtable:
                                    result_dict[col_value_name] = col_value_subtable
                            else:
                                document[column_name] = value

                        else:
                            document[column_name] = value
                            if table not in tabelas_verificar_mongo:
                                tabelas_verificar_mongo.append(table)
                    elif fk_info and fk_info[0] != table:
                        document[column_name] = value
                        if table not in tabelas_verificar_mongo:
                            tabelas_verificar_mongo.append(table)
                    else:
                        if convert_value(value):
                            document[column_name] = convert_value(value)
                        else:
                            document[column_name] = value
            mongo_collection.insert_one(document)
            # print(document)

    # for table in tabelas_verificar_mongo:
    #     # Buscar estrutura da tabela
    #     pg_cursor6 = pg_connection.cursor()
    #     pg_cursor6.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='{table}';")
    #     columns = {row[0]: row[1] for row in pg_cursor6.fetchall()}
    #
    #     pg_cursor7 = pg_connection.cursor()
    #     pg_cursor7.execute(f"SELECT * FROM {table};")
    #     data = pg_cursor7.fetchall()
    #     for row in data:
    #
    #         for i, value in enumerate(row):
    #             column_name = pg_cursor.description[i].name
    #             pg_cursor5 = pg_connection.cursor()
    #             pg_cursor5.execute(
    #                 f"SELECT ccu.table_name AS referenced_table, ccu.column_name AS referenced_column FROM information_schema.key_column_usage kcu JOIN information_schema.constraint_column_usage ccu ON kcu.constraint_name = ccu.constraint_name WHERE kcu.table_name = '{table}' AND kcu.column_name = '{column_name}'")
    #             fk_info = pg_cursor5.fetchone()
    #             if fk_info and fk_info[0] != table:
    #                 referenced_collection = mongo_db[fk_info[0]]
    #                 matching_document = referenced_collection.find_one({fk_info[1]: value})
    #
    #                 if matching_document:
    #                     mongo_db[table].update_one(matching_document,  {"$set": {fk_info[1]: ObjectId(matching_document['_id'])}})

except Exception as e:
    print("Erro:", e)
    traceback_str = traceback.format_exc()

    # Imprime o traceback
    print(traceback_str)
finally:
    pg_cursor.close()
    pg_connection.close()
    mongo_client.close()
