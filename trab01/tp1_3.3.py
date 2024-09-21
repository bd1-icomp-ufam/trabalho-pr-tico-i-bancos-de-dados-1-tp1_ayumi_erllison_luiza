import psycopg2
import os
from tabulate import tabulate

# Variáveis Globais
mensagemVoltar = 'Digite (v) para voltar e (x) para sair: '
conn = None
dashboard_query = {
    'a': """
            (
                SELECT 'MAIOR' as tipo,
                * FROM review
                WHERE idproduct = %s
                ORDER BY helpful DESC, rating DESC
                LIMIT 5
            )
            UNION ALL
            (
                SELECT 'MENOR' as tipo,
                * FROM review
                WHERE idproduct = %s
                ORDER BY helpful DESC, rating ASC
                LIMIT 5
            )
            ORDER BY tipo, helpful desc;
        """,

    'b': """
            SELECT produtoSimilares .*
            FROM produto p
            JOIN similares ps ON ps.asinpai = p.asin
            JOIN produto produtoSimilares ON produtoSimilares.asin = ps.asinsimilar
            WHERE produtoSimilares.salesrank < p.salesrank AND p.id = %s;
        """,

    'c': """
            SELECT data, count(*) as qntdReview, round(avg(rating), 4) as mediaAvaliacaoDia 
            FROM review 
            WHERE idproduct = %s
            GROUP BY data 
            ORDER BY data;
        """,

    'd': """
            WITH RankedProducts AS (
                SELECT grupo.name AS nomeGrupo,
                    ROW_NUMBER() OVER (PARTITION BY produto.idgroup ORDER BY produto.salesrank) AS ranking,
                    produto.*
                FROM produto
                INNER JOIN grupo ON produto.idgroup = grupo.name
                WHERE produto.salesrank > 0
            )
            SELECT *
            FROM RankedProducts
            WHERE ranking <= 10
            ORDER BY nomeGrupo, ranking;
        """,

    'e': """
            SELECT asin, title, idgroup, avgProductRating
            FROM 
                (
                    SELECT asin, title, idgroup, AVG(r.rating) AS avgProductRating 
                    FROM produto p 
                    INNER JOIN review r ON r.idproduct = p.id AND r.rating > 4 AND r.helpful > 0
                    GROUP BY asin, title, idgroup
                ) 
            AS aux_reviews
            ORDER BY avgProductRating DESC
            LIMIT 10;
        """,

    'f': """
            WITH id_categories AS (
                SELECT round(AVG(review.helpful), 4) AS media_helpful, produtocategoria.idcategory
                FROM review
                INNER JOIN produto ON review.idproduct = produto.id
                INNER JOIN produtocategoria ON produto.id = produtocategoria.idproduct
                WHERE review.rating >= 4
                GROUP BY produtocategoria.idcategory
                ORDER BY AVG(review.helpful) DESC
                LIMIT 5
            )
            SELECT categoria.name, categoria.id, id_categories.media_helpful
            FROM categoria
            JOIN id_categories ON categoria.id = id_categories.idcategory
            ORDER BY id_categories.media_helpful DESC;
        """,

    'g': """
            SELECT * 
            FROM 
                (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY idgroup ORDER BY count_customer_reviews DESC) AS rows 
                    FROM 
                    (
                        SELECT iduser, idgroup, COUNT(iduser) count_customer_reviews 
                        FROM produto p 
                        INNER JOIN review r ON r.idproduct = p.id 
                        GROUP BY iduser, idgroup
                    ) AS aux_reviews
                ) 
            AS aux WHERE rows <= 10;
        """
}

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

# Função para executar as consultas SQL
def query(sql, params=None):
    """Função genérica para executar consultas SQL."""
    global conn
    try:
        cur = conn.cursor()

        # Executa a consulta com ou sem parâmetros
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)

        result = cur.fetchall()
        cur.close()
        conn.commit()

        return result

    except psycopg2.DatabaseError as error:
        print("** Erro ao executar a consulta:", error)
        return None

# Função para limpar o terminal
def clearTerminal():
    os.system('clear')
    os.system('cls' if os.name == 'nt' else 'clear')

# Função para formatar o resultado da consulta A
def formatarResultadoA(resultado):
    """Formata o resultado da consulta no formato desejado."""
    
    # Contar quantos registros têm o tipo 'MAIOR'
    count_maiores = sum(1 for row in resultado if row[0] == 'MAIOR')

    # Imprimir o cabeçalho para as maiores avaliações
    print("-----------Comentarios com mais úteis e com mais avaliações--------------\n")

    # Iterar sobre o resultado e imprimir as maiores avaliações
    for i, row in enumerate(resultado):
        tipo = row[0]  # 'MAIOR' ou 'MENOR'
        id_user = row[1]
        data_avaliacao = row[3]
        rating = row[4]
        helpful = row[6]

        # Formatação da nota em forma de estrelas
        estrelas = '*' * int(rating)

        # Se chegar à primeira avaliação com tipo 'MENOR', imprimir o cabeçalho para menores
        if i == count_maiores:
            print("-----------Comentarios com mais úteis e com menos avaliações--------------\n")

        # Imprimir o resultado formatado
        print(f"Usuário: {id_user}")
        print(f"Avaliado em {data_avaliacao.strftime('%d de %B de %Y')}")
        print(estrelas)
        print(f"{helpful} pessoas acharam isso útil\n")

# Função para formatar o resultado da consulta B
def formatarResultadoB(resultado, idProduct):
    """Formata o resultado da consulta no formato desejado usando tabulate."""
    
    print(f"O produto {idProduct} tem os seguintes similares com rank maior que o seu:\n")
    
    # Preparar os dados para exibição
    headers = ['ID', 'Nome', 'Classificação']
    tabela_dados = []

    # Iterar sobre o resultado (produtos similares) e preparar os dados
    for row in resultado:
        id_produto = row[0]
        nome_produto_similar = row[2]
        salesrank_produto_similar = row[3]

        # Adicionar os dados à tabela
        tabela_dados.append([id_produto, nome_produto_similar, salesrank_produto_similar])

    # Exibir a tabela formatada
    print(tabulate(tabela_dados, headers=headers, tablefmt="fancy_grid"))
    print("\n")

# Função para formatar o resultado da consulta C
def formatarResultadoC(resultado, idProduct):
    print(f"A evolução diária das médias de avaliação do produto {idProduct} é:\n")
            
    # Preparar os dados para exibição na tabela
    headers = ['Data', 'Quantidade de Reviews', 'Média de Avaliação Diária']
    tabela_dados = []
            
    for row in resultado:
        data = row[0].strftime('%d/%m/%Y')  # Formatar a data como string (dd/mm/yyyy)
        qntdReview = row[1]
        mediaAvaliacaoDia = row[2]
                
        # Adicionar os dados à tabela
        tabela_dados.append([data, qntdReview, mediaAvaliacaoDia])

    # Exibir a tabela formatada
    print(tabulate(tabela_dados, headers=headers, tablefmt="fancy_grid"))
    print("\n")

# Função para formatar o resultado da consulta D
def formatarResultadoD(resultado):
    """Formata e exibe os 10 produtos líderes de venda por grupo usando tabulate."""
    
    grupo_atual = None
    tabela_dados = []

    # Itera sobre o resultado da consulta
    for row in resultado:
        nome_grupo = row[0]  # O nome do grupo
        id_produto = row[2]  # O ID do produto
        asin = row[3]        # O ASIN do produto
        title = row[4]       # O título do produto
        salesrank = row[5]   # O ranking de vendas do produto

        # Se o grupo mudou, imprime a tabela para o grupo anterior (se houver) e reinicia a tabela
        if nome_grupo != grupo_atual:
            if grupo_atual is not None:
                # Exibe a tabela do grupo anterior
                print(tabulate(tabela_dados, headers=["ID", "ASIN", "Nome", "Classificação"], tablefmt="fancy_grid"))
                print("-" * 80)

            # Atualiza o nome do grupo atual e limpa os dados da tabela
            grupo_atual = nome_grupo
            tabela_dados = []

            # Exibe o cabeçalho para o novo grupo
            print(f"\nProdutos líderes de venda do grupo {nome_grupo}")
            print("-" * 80)

        # Adiciona os dados do produto atual à lista de dados
        tabela_dados.append([id_produto, asin, title, salesrank])

    # Exibe a última tabela após o loop, se houver dados
    if tabela_dados:
        print(tabulate(tabela_dados, headers=["ID", "ASIN", "Nome", "Classificação"], tablefmt="fancy_grid"))
        print("-" * 80)

# Função para formatar o resultado da consulta E
def formatarResultadoE(resultado):
    """Formata e exibe os 10 produtos com a maior média de avaliações úteis e com nota maior que 4."""

    # Se o resultado não estiver vazio, formata e exibe
    if resultado:
        # Preparar os dados para a tabela
        tabela_dados = []
        for row in resultado:
            asin = row[0]  # O código único do produto (ASIN)
            title = row[1]  # O título do produto
            nomeGrupo = row[2]  # O nome do grupo ao qual o produto pertence
            avgProductRating = row[3]  # A média das avaliações do produto

            # Adicionar cada produto na lista de dados para exibição
            tabela_dados.append([asin, title, nomeGrupo, avgProductRating])

        # Exibir a tabela formatada usando 'tabulate'
        print(tabulate(tabela_dados, headers=["ASIN", "Nome", "Grupo", "Média de Avaliação"], tablefmt="fancy_grid"))

    else:
        print("Nenhum resultado encontrado.")
    
    print("-" * 80)

# Função para formatar o resultado da consulta F
def formatarResultadoF(resultado):
    """Formata e exibe as 5 categorias com a maior média de avaliações úteis positivas."""
    
    # Verifica se há resultados
    if not resultado:
        print("Nenhum resultado encontrado.")
        return

    # Define os cabeçalhos da tabela
    headers = ["ID Categoria", "Nome da Categoria", "Média de Avaliações Úteis"]

    # Formata os dados para a tabela
    tabela = [
        [row[1], row[0], row[2]]  # row[1] = ID, row[0] = Nome da categoria, row[2] = Média útil
        for row in resultado
    ]

    # Exibe a tabela formatada com 'tabulate'
    print(tabulate(tabela, headers, tablefmt="fancy_grid"))

    # Exibe uma linha de separação final
    print("-" * 80)

# Função para formatar o resultado da consulta G
def formatarResultadoG(resultado):
    """Formata e exibe a tabela dos 10 principais clientes por grupo de produto usando tabulate."""
    
    grupo_atual = None
    tabela_dados = []

    # Itera sobre o resultado da consulta
    for row in resultado:
        id_grupo = row[1]  # O ID do grupo
        id_usuario = row[0]  # O ID do usuário
        qntd_comentarios = row[2]  # Quantidade de comentários
        ranking = row[3]  # O ranking do cliente no grupo

        # Se o grupo mudou, imprime a tabela para o grupo anterior (se houver) e reinicia a tabela
        if id_grupo != grupo_atual:
            if grupo_atual is not None:
                # Exibe a tabela do grupo anterior
                print(tabulate(tabela_dados, headers=["ID Usuário", "ID Grupo", "Quantidade de Comentários", "Ranking"], tablefmt="fancy_grid"))
                print("-" * 80)

            # Atualiza o nome do grupo atual e limpa os dados da tabela
            grupo_atual = id_grupo
            tabela_dados = []

            # Exibe o cabeçalho para o novo grupo
            print(f"\n10 principais clientes que comentaram no grupo {id_grupo}")
            print("-" * 80)

        # Adiciona os dados do usuário atual à lista de dados
        tabela_dados.append([id_usuario, id_grupo, qntd_comentarios, ranking])

    # Exibe a última tabela após o loop, se houver dados
    if tabela_dados:
        print(tabulate(tabela_dados, headers=["ID Usuário", "ID Grupo", "Quantidade de Comentários", "Ranking"], tablefmt="fancy_grid"))
        print("-" * 80)

# Função para executar a consulta A
def consultaA():
    print("\n------------------------------------------------------------------------------------\n")
    print('Listar os 5 comentários mais úteis e com maior avaliação e  \nos 5 comentários mais úteis e com menor avaliação')
    print("\n------------------------------------------------------------------------------------\n")
    
    idProduct = input('Digite o id do produto: ')
    print("\n------------------------------------------------------------------------------------\n")

    # Executar a consulta 'a' (maiores e menores avaliações)
    resultadoA = query(dashboard_query['a'], (idProduct, idProduct))

    if resultadoA:
        # Formatar e exibir os resultados da consulta 'a'
        formatarResultadoA(resultadoA)
    else:
        print("Nenhum resultado encontrado para o produto " + idProduct + ".")

# Função para executar a consulta B
def consultaB():
    print("\n------------------------------------------------------------------------------------\n")
    print('Listar os produtos similares com maiores vendas do que ele')
    print("\n------------------------------------------------------------------------------------\n")
    
    idProduct = input('Digite o id do produto: ')
    print("\n------------------------------------------------------------------------------------\n")

    # Executar a consulta 'b'
    resultadoB = query(dashboard_query['b'], (idProduct,))

    if resultadoB:
        # Formatar e exibir os resultados da consulta 'b'
        formatarResultadoB(resultadoB, idProduct)
    else:
        print("Nenhum resultado encontrado para o produto " + idProduct + ".")

# Função para executar a consulta C
def consultaC():
    print("\n------------------------------------------------------------------------------------\n")
    print('Mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo \ncoberto no arquivo de entrada')
    print("\n------------------------------------------------------------------------------------\n")
    
    idProduct = input('Digite o id do produto: ')
    print("\n------------------------------------------------------------------------------------\n")

    # Executar a consulta 'c'
    resultadoC = query(dashboard_query['c'], (idProduct,))

    if resultadoC:
        # Formatar e exibir os resultados da consulta 'c'
        formatarResultadoC(resultadoC, idProduct)
    else:
        print(f"Nenhuma avaliação encontrada para o produto {idProduct}.")

# Função para executar a consulta D
def consultaD():
    print("\n------------------------------------------------------------------------------------\n")
    print('Listar os 10 produtos líderes de venda em cada grupo de produtos')
    print("\n------------------------------------------------------------------------------------\n")
    
    # Executar a consulta 'D'
    resultadoD = query(dashboard_query['d'])

    formatarResultadoD(resultadoD)

# Função para executar a consulta E
def consultaE():
    print("\n------------------------------------------------------------------------------------\n")
    print('Listar os 10 produtos com a maior média de avaliações úteis positivas por produto')
    print("\n------------------------------------------------------------------------------------\n")
    
    # Executar a consulta 'E'
    resultadoE = query(dashboard_query['e'])

    formatarResultadoE(resultadoE)

# Função para executar a consulta F
def consultaF():
    print("\n------------------------------------------------------------------------------------\n")
    print('Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto')
    print("\n------------------------------------------------------------------------------------\n")
    
    # Executar a consulta 'F'
    resultadoF = query(dashboard_query['f'])

    formatarResultadoF(resultadoF)

# Função para executar a consulta G
def consultaG():
    print("\n------------------------------------------------------------------------------------\n")
    print('Listar os 10 clientes que mais fizeram comentários por grupo de produto')
    print("\n------------------------------------------------------------------------------------\n")
    
    # Executar a consulta 'G'
    resultadoG = query(dashboard_query['g'])

    formatarResultadoG(resultadoG)

# Executar Opções
def executarConsulta(opcao):
    funcaoOpcao = None

    if opcao == 'a':
        funcaoOpcao = consultaA
    elif opcao == 'b':
        funcaoOpcao = consultaB
    elif opcao == 'c':
        funcaoOpcao = consultaC
    elif opcao == 'd':
        funcaoOpcao = consultaD
    elif opcao == 'e':
        funcaoOpcao = consultaE
    elif opcao == 'f':
        funcaoOpcao = consultaF
    elif opcao == 'g':
        funcaoOpcao = consultaG
    elif opcao == 'x':
        exit()

    clearTerminal()
    funcaoOpcao()

    loopOpcao = input(mensagemVoltar)
    while loopOpcao != 'v':
        if loopOpcao == 'v':
            break 
        elif loopOpcao == 'x':
            exit()
        else:
            loopOpcao = input('Opção inválida, digite novamente: ')

# Função para verificar opções válidas
def opcaoValida(opcao):
    return (opcao == 'a' or opcao == 'b' or opcao == 'c' or opcao == 'd' or opcao == 'e'
            or opcao == 'f' or opcao == 'g' or opcao == 'x')

# Função principal
def main():
    global conn
    conn = conectarAoBanco()

    if conn is None:
        return

    try:
        while True:  # Loop principal para sempre voltar ao menu
            print("\n------------------------------------------------------------------------------------\n")
            print("Bem-vindo ao Dashboard :)")
            print("\n------------------------------------------------------------------------------------\n")
            print('Escolha uma das opções abaixo:')
            print('''a) Listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação;''')
            print('''b) Listar os produtos similares com maiores vendas do que ele;''')
            print('''c) Mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada;''')
            print('''d) Listar os 10 produtos líderes de venda em cada grupo de produtos;''')
            print('''e) Listar os 10 produtos com a maior média de avaliações úteis positivas por produto;''')
            print('''f) Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto;''')
            print('''g) Listar os 10 clientes que mais fizeram comentários por grupo de produto.''')
            print('''x) Sair do programa.''')

            opcao = input('Digite a opção desejada: ')

            while not opcaoValida(opcao):
                opcao = input('Opção inválida, digite novamente: ')

            # Quando a opção for válida, chamamos a função para executar a consulta
            if opcao == 'x':
                break
            else:
                executarConsulta(opcao)

    finally:
        # Fechar a conexão ao encerrar o programa
        if conn:
            conn.close()
            print("Conexão com o banco de dados encerrada.")

if __name__ == '__main__':
    main()