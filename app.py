from flask import Flask, request, render_template
import psycopg2
from pymongo import MongoClient
from decimal import Decimal
from datetime import date
import os

app = Flask(__name__)

# Rota para a página de upload
@app.route('/')
def upload_file():
    return render_template('upload.html')

# Rota para processar o upload e inserir no MongoDB remoto
@app.route('/upload', methods=['POST'])
def upload_and_insert():
    uploaded_file = request.files['file']

    if uploaded_file.filename != '':
        # Salvar o arquivo temporariamente
        temp_filename = os.path.join('uploads', uploaded_file.filename)
        uploaded_file.save(temp_filename)

        # Obter informações de conexão inseridas pelo usuário
        pg_host = request.form['pg_host']
        pg_dbname = request.form['pg_dbname']
        pg_user = request.form['pg_user']
        pg_password = request.form['pg_password']
        mongo_host = request.form['mongo_host']
        mongo_dbname = request.form['mongo_dbname']
        mongo_user = request.form['mongo_user']
        mongo_password = request.form['mongo_password']

        # Conexão com o banco de dados PostgreSQL remoto
        pg_config = {
            'dbname': pg_dbname,
            'user': pg_user,
            'password': pg_password,
            'host': pg_host
        }
        pg_connection = psycopg2.connect(**pg_config)

        try:
            pg_cursor = pg_connection.cursor()

            # Lógica para ler o arquivo e inserir no MongoDB remoto
            # ...
            # (Siga o exemplo anterior para buscar dados e estrutura do PostgreSQL
            #  e inserir no MongoDB)
            # ...

            return 'Arquivo enviado e inserido no MongoDB remoto com sucesso!'
        except Exception as e:
            return f'Erro: {e}'
        finally:
            pg_cursor.close()
            pg_connection.close()

        # Remover o arquivo temporário
        os.remove(temp_filename)

    return 'Nenhum arquivo enviado.'

if __name__ == '__main__':
    app.run(debug=True)
