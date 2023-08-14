import psycopg2
from pymongo import MongoClient
from decimal import Decimal
from datetime import date
import traceback
from config import PG_CONFIG, MONGO_CONFIG
from utils import convert_value

# Conexão com o banco de dados MongoDB
mongo_client = MongoClient(MONGO_CONFIG['host'], MONGO_CONFIG['port'])
mongo_db = mongo_client[MONGO_CONFIG['dbname']]

# Conexão com o banco de dados PostgreSQL
pg_connection = psycopg2.connect(**PG_CONFIG)

try:
    # Buscar tabelas do PostgreSQL
    pg_cursor = pg_connection.cursor()
    pg_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' and table_name='customer';")
    tables = [table[0] for table in pg_cursor.fetchall()]

    for table in tables:
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

                if fk_info and fk_info[0] != table:
                    referenced_table = fk_info[0]
                    referenced_column = fk_info[1]

                    if column_name.endswith('_id'):
                        foreign_table = column_name[:-3]
                    else:
                        foreign_table = column_name

                    foreign_object_id = value
                    pg_cursor3 = pg_connection.cursor()
                    pg_cursor3.execute(f"SELECT * FROM {referenced_table} WHERE {referenced_column} = {value};")
                    foreign_object = pg_cursor3.fetchone()
                    if foreign_object:
                        foreign_object_list = list(foreign_object)
                        for key, valor in enumerate(foreign_object_list):
                            if convert_value(valor):
                                foreign_object_list[key] = convert_value(valor)
                        document[column_name] = tuple(foreign_object_list)
                    else:
                        document[column_name] = value
                else:
                    if convert_value(value):
                        document[column_name] = convert_value(value)
                    else:
                        document[column_name] = value

            mongo_collection.insert_one(document)
            # print(document)

except Exception as e:
    print("Erro:", e)
    traceback_str = traceback.format_exc()

    # Imprime o traceback
    print(traceback_str)
finally:
    pg_cursor.close()
    pg_cursor2.close()
    pg_cursor3.close()
    pg_connection.close()
    mongo_client.close()
