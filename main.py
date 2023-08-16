import psycopg2
from bson import BSON
from pymongo import MongoClient
from decimal import Decimal
from datetime import date
import traceback
from config import PG_CONFIG, MONGO_CONFIG, QTDE_MIN_FOREIGN_KEY_AGREGACAO
from utils import convert_value, update_collection

# Conexão com o banco de dados MongoDB
mongo_client = MongoClient(MONGO_CONFIG['host'], MONGO_CONFIG['port'])
mongo_db = mongo_client[MONGO_CONFIG['dbname']]

# Conexão com o banco de dados PostgreSQL
pg_connection = psycopg2.connect(**PG_CONFIG)

try:
    tabelas_verificadas = list()
    tabelas_verificar_mongo = list()
    # Buscar tabelas do PostgreSQL
    pg_cursor = pg_connection.cursor()
    pg_cursor.execute("SELECT t.table_name, COUNT(constraint_name) AS num_foreign_keys FROM information_schema.tables t LEFT JOIN information_schema.table_constraints tc ON t.table_name = tc.table_name WHERE t.table_schema = 'public' AND constraint_type = 'FOREIGN KEY' GROUP BY t.table_name ORDER BY num_foreign_keys DESC;")
    tables = [table[0] for table in pg_cursor.fetchall()]

    for table in tables:
        if table in tabelas_verificadas:
            continue
        # Buscar estrutura da tabela
        pg_cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='{table}';")
        columns = {row[0]: row[1] for row in pg_cursor.fetchall()}

        # Buscar dados da tabela
        pg_cursor.execute(f"SELECT * FROM {table};")
        data = pg_cursor.fetchall()

        # Inserir dados no MongoDB
        mongo_collection = mongo_db[table]
        for row in data:
            document = {}
            for i, value in enumerate(row):
                column_name = pg_cursor.description[i].name
                data_type = columns.get(column_name)
                pg_cursor2 = pg_connection.cursor()
                pg_cursor2.execute(f"SELECT ccu.table_name AS referenced_table, ccu.column_name AS referenced_column FROM information_schema.key_column_usage kcu JOIN information_schema.constraint_column_usage ccu ON kcu.constraint_name = ccu.constraint_name WHERE kcu.table_name = '{table}' AND kcu.column_name = '{column_name}'")
                fk_info = pg_cursor2.fetchone()

                pg_cursor4 = pg_connection.cursor()
                pg_cursor4.execute(
                    f"SELECT COUNT(constraint_name) AS num_foreign_keys FROM information_schema.tables t LEFT JOIN information_schema.table_constraints tc ON t.table_name = tc.table_name WHERE t.table_schema = 'public' AND constraint_type = 'FOREIGN KEY' AND t.table_name = '{table}' GROUP BY t.table_name")
                num_foreign_keys = pg_cursor4.fetchone()[0]
                if fk_info and fk_info[0] != table and num_foreign_keys >= QTDE_MIN_FOREIGN_KEY_AGREGACAO:
                    referenced_table = fk_info[0]
                    referenced_column = fk_info[1]

                    if column_name.endswith('_id'):
                        foreign_table = column_name[:-3]
                    else:
                        foreign_table = column_name

                    foreign_object_id = value
                    pg_cursor3 = pg_connection.cursor()
                    pg_cursor3.execute(f"SELECT * FROM {referenced_table} WHERE {referenced_column} = {value};")
                    column_names = [desc[0] for desc in pg_cursor3.description]

                    foreign_object = pg_cursor3.fetchone()
                    if foreign_object:
                        result_dict = {col_name: col_value for col_name, col_value in zip(column_names, foreign_object)}
                        for key, valor in result_dict.items():
                            if convert_value(valor):
                                result_dict[key] = convert_value(valor)
                        document[column_name] = result_dict
                        if referenced_table not in tabelas_verificadas:
                            tabelas_verificadas.append(referenced_table)
                    else:
                        document[column_name] = value
                elif fk_info and fk_info[0] != table and num_foreign_keys < QTDE_MIN_FOREIGN_KEY_AGREGACAO:
                    document[column_name] = BSON.encode({column_name: value})
                    if table not in tabelas_verificar_mongo:
                        tabelas_verificar_mongo.append(table)
                else:
                    if convert_value(value):
                        document[column_name] = convert_value(value)
                    else:
                        document[column_name] = value

            mongo_collection.insert_one(document)
            # print(document)

    for table in tabelas_verificar_mongo:
        # Buscar estrutura da tabela
        pg_cursor6 = pg_connection.cursor()
        pg_cursor6.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='{table}';")
        columns = {row[0]: row[1] for row in pg_cursor6.fetchall()}

        pg_cursor7 = pg_connection.cursor()
        pg_cursor7.execute(f"SELECT * FROM {table};")
        data = pg_cursor7.fetchall()
        for row in data:

            for i, value in enumerate(row):
                column_name = pg_cursor.description[i].name
                pg_cursor5 = pg_connection.cursor()
                pg_cursor5.execute(
                    f"SELECT ccu.table_name AS referenced_table, ccu.column_name AS referenced_column FROM information_schema.key_column_usage kcu JOIN information_schema.constraint_column_usage ccu ON kcu.constraint_name = ccu.constraint_name WHERE kcu.table_name = '{table}' AND kcu.column_name = '{column_name}'")
                fk_info = pg_cursor5.fetchone()
                if fk_info and fk_info[0] != table:
                    referenced_collection = mongo_db[fk_info[0]]
                    matching_document = referenced_collection.find_one({fk_info[1]: value})

                    if matching_document:
                        mongo_db[table].update_one(matching_document,  {"$set": {fk_info[1]: BSON.encode({fk_info[1]:matching_document['_id']})}})

except Exception as e:
    print("Erro:", e)
    traceback_str = traceback.format_exc()

    # Imprime o traceback
    print(traceback_str)
finally:
    pg_cursor.close()
    pg_cursor2.close()
    pg_cursor3.close()
    pg_cursor4.close()
    pg_connection.close()
    mongo_client.close()
