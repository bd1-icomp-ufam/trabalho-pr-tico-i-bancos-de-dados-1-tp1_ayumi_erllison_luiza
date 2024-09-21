import os
import psycopg2
import re

# Função para conectar ao banco de dados
def conectarAoBanco():
    try:
        # Conectar ao banco de dados PostgreSQL
        conexao = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            database=os.getenv('DB_NAME', 'products_amazon'),
            user=os.getenv('DB_USER', 'ame'),
            password=os.getenv('DB_PASS', 'ame1234'),
            port="5433"
        )

        print("Conexão com o banco de dados estabelecida com sucesso.")
        return conexao
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erro ao conectar ao banco de dados: {error}")
        return None

# Função para criar o esquema de tabelas
def criarEsquema(conexao):
    try:
        cursor = conexao.cursor()

        # Criar a tabela grupo
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS grupo (
                name VARCHAR(30) PRIMARY KEY
            );
        ''')

        # Criar a tabela produto
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS produto (
                id INT PRIMARY KEY,
                asin VARCHAR(10) UNIQUE NOT NULL,
                title VARCHAR(512),
                salesrank INT,
                idgroup VARCHAR(30),
                CONSTRAINT fk_group FOREIGN KEY(idgroup) REFERENCES grupo(name)
            );
        ''')

        # Criar a tabela similares
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS similares (
                asinPai VARCHAR(10),
                asinSimilar VARCHAR(10),
                PRIMARY KEY (asinPai, asinSimilar)
            );
        ''')

        # Criar a tabela categoria
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS categoria (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                idPai INT
            );
        ''')

        # Criar a tabela review
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS review (
                idUser VARCHAR(25),
                idProduct INT,
                data DATE,
                rating SMALLINT,
                votes INT,
                helpful INT,
                PRIMARY KEY (idUser, idProduct)
            );
        ''')

        # Criar a tabela user
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS "user" (
                idUser VARCHAR(25) PRIMARY KEY
            );
        ''')

        # Criar a tabela ProdutoCategoria
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ProdutoCategoria (
                idProduct INT,
                idCategory INT,
                PRIMARY KEY (idProduct, idCategory)
            );
        ''')

        # Commit das mudanças
        conexao.commit()
        print("Esquema de banco de dados criado com sucesso.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erro ao criar o esquema de banco de dados: {error}")

# Função para inserir dados em lotes
def inserirDados(conexao, dados):
    insert_commands = {
        "grupo": '''
            INSERT INTO grupo(name) 
            VALUES (%s) 
            ON CONFLICT (name) DO NOTHING
        ''',
        "produto": '''
            INSERT INTO produto(id, asin, title, salesrank, idgroup)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (asin) DO NOTHING
        ''',
        "similares": '''
            INSERT INTO similares(asinPai, asinSimilar)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        ''',
        "categoria": '''
            INSERT INTO categoria(id, name, idPai)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        ''',
        "review": '''
            INSERT INTO review(idUser, idProduct, data, rating, votes, helpful)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        ''',
        "user": '''
            INSERT INTO "user"(idUser)
            VALUES (%s)
            ON CONFLICT DO NOTHING
        ''',
        "produtoCategoria": '''
            INSERT INTO produtoCategoria(idProduct, idCategory)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        '''
    }

    try:
        cursor = conexao.cursor()

        # Inserir grupos
        if dados["grupo"]:
            # Converte o set em uma lista de tuplas
            gruposParaInserir = [(grupo,) for grupo in dados["grupo"]]
            cursor.executemany(insert_commands["grupo"], gruposParaInserir)

        # Inserir produtos
        if dados["produto"]:
            cursor.executemany(insert_commands["produto"], dados["produto"])

        # Inserir similares
        if dados["similares"]:
            cursor.executemany(insert_commands["similares"], dados["similares"])

        # Inserir categorias
        if dados["categoria"]:
            cursor.executemany(insert_commands["categoria"], dados["categoria"])

        # Inserir produtoCategoria
        if dados["produtoCategoria"]:
            cursor.executemany(insert_commands["produtoCategoria"], dados["produtoCategoria"])

        # Inserir usuários
        if dados["user"]:
            cursor.executemany(insert_commands["user"], dados["user"])

        # Inserir reviews
        if dados["review"]:
            cursor.executemany(insert_commands["review"], dados["review"])

        conexao.commit()
        print("Dados inseridos com sucesso.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erro ao inserir dados: {error}")
        conexao.rollback()
    finally:
        cursor.close()

# Função para processar arquivo e carregar dados em chunks
def processarArquivo(filepath, conexao):
    # Inicializar estrutura de dados para chunking
    chunk_data = {
        "grupo": set(),
        "produto": [],
        "similares": set(),
        "categoria": [],
        "review": [],
        "user": set(),
        "produtoCategoria": set()
    }

    product_data = None
    padrao_categoria = re.compile(r"^(.+)\[(\d+)]$")

    with open(filepath, 'r', encoding='utf-8') as file:
        for line in file:
            info = line.split()
            if len(info) < 1:
                continue

            # Processar informações de produto
            if info[0].startswith('Id:'):
                if product_data:
                    chunk_data['produto'].append(product_data)
                product_data = [int(info[1]), None, None, None, None]  # id, asin, title, salesrank, idgroup
            elif info[0].startswith('ASIN:'):
                product_data[1] = info[1]
            elif info[0].startswith('title:'):
                product_data[2] = " ".join(info[1:])
            elif info[0].startswith('group:'):
                group_name = " ".join(info[1:])
                chunk_data['grupo'].add((group_name))
                product_data[4] = group_name
            elif info[0].startswith('salesrank:'):
                product_data[3] = int(info[1])

            # Processar categorias
            elif info[0].startswith('categories:'):
                total_categories = int(info[1])
                categoria_pai = None  # Inicializa a variável para armazenar a categoria pai
                for _ in range(total_categories):
                    line = next(file)
                    categorias = line.split('|')
                    for i, cat in enumerate(categorias):
                        match = padrao_categoria.match(cat)
                        if match:
                            category_name = match.group(1).strip()
                            category_id = int(match.group(2).strip())
                            if i == 0:
                                # A primeira categoria não tem pai, é a raiz
                                chunk_data['categoria'].append((category_id, category_name, None))  # Sem pai (None)
                            else:
                                # As próximas categorias são filhas da categoria anterior
                                chunk_data['categoria'].append((category_id, category_name, categoria_pai))
                            # Atualiza a categoria pai para a próxima iteração
                            categoria_pai = category_id
                            # Associar o produto à categoria, mas usando set() para evitar duplicatas
                            chunk_data['produtoCategoria'].add((product_data[0], category_id))

            # Processar similares
            elif info[0].startswith('similar:'):
                count_similar = int(info[1])
                for i in range(count_similar):
                    chunk_data['similares'].add((product_data[1], info[2 + i]))

            # Processar reviews
            elif info[0].startswith('reviews:'):
                # Extrai o total de reviews a partir da linha
                total_reviews = int(info[4])  # O número de reviews baixadas para aquele produto

                # Agora processa cada review individual
                for _ in range(total_reviews):
                    review_line = next(file).strip()  # Lê a linha da review e remove espaços em branco nas bordas

                    # Divide a linha usando espaços simples como delimitadores
                    review_info = review_line.split()

                    # Verifica se a linha contém os dados esperados
                    if len(review_info) < 9:
                        continue  # Pula essa linha se não tiver os campos necessários

                    # Extrai os campos com base em sua posição relativa
                    review_date = review_info[0]  # A primeira parte é a data (ex: "2000-7-28")
                    review_user = review_info[2]  # Após "customer:", o próximo campo é o ID do usuário (ex: "A2JW67OY8U6HHK")
                    try:
                        review_rating = int(review_info[4])  # Após "rating:", o próximo campo é o valor do rating (ex: 5)
                    except ValueError:
                        print(f"Erro: rating inválido para o produto {product_data[1]}: {info[1]}")
                    review_votes = int(review_info[6])  # Após "votes:", o próximo campo é o número de votos (ex: 10)
                    review_helpful = int(review_info[8])  # Após "helpful:", o próximo campo é o número de votos úteis (ex: 9)

                    # Adiciona o usuário à lista de usuários
                    chunk_data['user'].add((review_user,))

                    # Adiciona a review à lista de reviews
                    chunk_data['review'].append((
                        review_user,    # ID do usuário
                        product_data[0],  # ID do produto (product_data[0] contém o ID do produto)
                        review_date,    # Data da review
                        review_rating,  # Rating da review
                        review_votes,   # Número de votos
                        review_helpful  # Votos úteis
                    ))

        # Inserir o último produto
        if product_data:
            chunk_data['produto'].append(product_data)

        # Visualizar os dados carregados antes da inserção
        print("Extração finalizada.")
        inserirDados(conexao, chunk_data)

# Função principal
def main():
    conn = conectarAoBanco()
    if conn:
        criarEsquema(conn)
        filepath = './amazon-meta.txt'
        processarArquivo(filepath, conn)
        conn.close()

# Ponto de entrada do script
if __name__ == '__main__':
    main()