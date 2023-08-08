import psycopg2
from pymongo import MongoClient
from decimal import Decimal
from datetime import date

# Configurações do banco de dados PostgreSQL
pg_config = {
    'dbname': 'dvdrental2',
    'user': 'postgres',
    'password': 'th32s7',
    'host': 'localhost'  # ou o endereço do servidor PostgreSQL
}

# Configurações do MongoDB
mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client['seu_banco_mongodb_1234']

# Conexão com o banco de dados PostgreSQL
pg_connection = psycopg2.connect(**pg_config)

try:
    # Buscar tabelas do PostgreSQL
    pg_cursor = pg_connection.cursor()
    pg_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
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
                pg_cursor.execute(f"SELECT ccu.table_name AS referenced_table, ccu.column_name AS referenced_column FROM information_schema.key_column_usage kcu JOIN information_schema.constraint_column_usage ccu ON kcu.constraint_name = ccu.constraint_name WHERE kcu.table_name = '{table}' AND kcu.column_name = '{column_name}'")
                fk_info = pg_cursor.fetchone()
                print(fk_info, value)
                # exit()
                if fk_info:
                    referenced_table = fk_info[0]
                    referenced_column = fk_info[1]
                    pg_cursor.execute(f"SELECT * FROM {referenced_table} where {referenced_column}= {value};")
                    referenced_row = pg_cursor.fetchone()
                    print(referenced_row)
                # Converter decimal.Decimal para float ou str
                if isinstance(value, Decimal):
                    document[column_name] = float(value)
                    # Converter datetime.date para string no formato ISO 8601
                elif isinstance(value, date):
                    document[column_name] = value.isoformat()
                # Converter memoryview para bytes
                elif isinstance(value, memoryview):
                    document[column_name] = bytes(value)
                else:
                    document[column_name] = {
                        "value": value,
                        "data_type": data_type
                    }
            mongo_collection.insert_one(document)

except Exception as e:
    print("Erro:", e)
finally:
    pg_cursor.close()
    pg_connection.close()
    mongo_client.close()
