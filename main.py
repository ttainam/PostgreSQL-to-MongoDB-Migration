import psycopg2
import networkx as nx

# Define a função para criar o grafo
def create_graph(conn):
    # Cria um cursor para executar consultas SQL
    cur = conn.cursor()

    # Obtém o nome de todas as tabelas do banco de dados e a quantidade de dados em cada tabela
    cur.execute("SELECT relname, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC")
    table_info = cur.fetchall()

    # Cria um grafo direcionado vazio usando o NetworkX
    G = nx.DiGraph()

    # Adiciona um nó para cada tabela
    for relname, n_live_tup in table_info:
        G.add_node(relname)
        # print(f"{relname} ({n_live_tup} registros)")

    # Adiciona uma aresta para cada relação entre tabelas (chave estrangeira)
    for table in G.nodes():
        cur.execute("SELECT tc.table_name, kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name \
                        FROM information_schema.table_constraints AS tc \
                        JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name \
                        JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name \
                        WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name=%s", [table])
        foreign_keys = cur.fetchall()
        for foreign_key in foreign_keys:
            print(foreign_key)
            G.add_edge(table, foreign_key[2])

    # # Desenha o grafo na tela
    # nx.draw(G, with_labels=True)

    # Imprime todas as arestas do grafo
    # print("Arestas do grafo:")
    # for edge in G.edges():
    #     print(edge)

    # EDGE(0) -> Tabela origem, EDGE(1) -> Tabela destino
    # Para cada EDGE(0) exitem Ns EDGE(1)
    # Ex: Select * from EDGE(0)
    #     Left Join EDGE(1) on ...
    #

    # Fecha o cursor e a conexão com o banco de dados
    cur.close()
    conn.close()

# Define as informações da conexão com o banco de dados
conn = psycopg2.connect(
    host="localhost",
    database="dvdrental2",
    user="postgres",
    password="th32s7"
)
# rodar
#mongo-connector -c config.json

# Cria o grafo
create_graph(conn)