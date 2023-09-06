
# Configurações do PostgreSQL
PG_CONFIG = {
    'dbname': 'dvdrental',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost'
}

# Configurações do MongoDB
MONGO_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'dbname': 'teste123'
}

# Insere o objectId nos campos Foreign Key que ficaram com ID.
INSERT_OBJECT_ID_REFERENCES = False

# Insere os campos com valores null
# Por conceito: bancos NoSQL não deveriam inserir valores não significativos.
INSERT_NULL_FIELDS = False
