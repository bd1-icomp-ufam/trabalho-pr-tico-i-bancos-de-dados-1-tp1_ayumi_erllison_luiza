# Trabalho 1 - Dashboard de Produtos Amazon

## Descrição

Este projeto implementa um dashboard que permite a manipulação de dados de produtos, categorias, avaliações e similaridades de produtos da Amazon. O sistema permite executar diversas consultas, como:

- Comentários mais úteis e com maior avaliação;
- Produtos similares com maiores vendas;
- Evolução diária das médias de avaliação de um produto;
- Produtos líderes de venda em cada grupo de produtos;
- Produtos com maior média de avaliações úteis;
- Categorias com a maior média de avaliações úteis positivas;
- Clientes que mais fizeram comentários por grupo de produto.

## Requisitos

### Docker

Certifique-se de ter o Docker e Docker Compose instalados no seu sistema. Para instalar, siga os tutoriais apropriados:

- [Instalando Docker no Windows](https://gitlab.com/paulonellessen/docker-saas/-/wikis/Instalando%20o%20Docker/Windows)
- [Instalando Docker no Ubuntu](https://gitlab.com/paulonellessen/docker-saas/-/wikis/Instalando%20o%20Docker/Ubuntu)

### Dependências do Python

Caso você opte por rodar o projeto sem Docker, será necessário instalar as seguintes dependências no seu ambiente local:

```bash
pip install psycopg2-binary tabulate
```

## Execução com Docker

1. **Subir o ambiente com Docker Compose**:
   Execute o comando abaixo no diretório do projeto:

   ```bash
   docker compose up --build
   ```

   Esse comando irá configurar o ambiente, incluindo o banco de dados PostgreSQL e instalar as dependências necessárias.

2. **Executar o arquivo principal**:
   Após o ambiente ser iniciado, rode o seguinte comando para iniciar o dashboard:

   ```bash
   docker exec -it trab1-db-1 python3 tp1_3.2.py
   ```

   Esse comando acessa o container da aplicação e executa o arquivo Python responsável pelo dashboard.

## Executando Localmente

Caso prefira executar o projeto sem Docker, siga as instruções abaixo:

1. **Instalar as dependências**:
   Execute o comando abaixo para instalar as dependências no seu ambiente Python local:

   ```bash
   pip install psycopg2 tabulate
   ```

2. **Configurar o banco de dados**:
   Certifique-se de que o PostgreSQL esteja rodando e configurado com as seguintes credenciais:

   - **Usuário**: `ame`
   - **Senha**: `ame1234`
   - **Banco de dados**: `products_amazon`
   - **Porta**: `5433`

3. **Executar os arquivos**:
   Rode os arquivos a seguir para iniciar o dashboard:

   ```bash
   python3 tp1_3.2.py
   ```

   ```bash
   python3 tp1_3.3.py
   ```
