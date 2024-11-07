# Luiza Labs - Case

## Pré-requisitos

Antes de começar, você precisará ter o docker instalado na sua máquina:

- [Docker](https://docs.docker.com/get-docker/)

# Requisitos

- Greenlet
- Numpy
- Pandas
- Psycopg2-binary
- Python-dateutil
- Pytz
- Six
- SQLAlchemy
- Typing_extensions
- Tzdata
- Pyspark

# Como executar

- Clone o repositório para sua máquina local
```
git clone git@github.com:descomplicandodados/luizalabs_teste.git
```
- Navegue até a pasta do projeto
```
cd luizalabs_yteste
```
- Execute o comando
```
docker-compose up -d 
```


# Funcionamento
- No desafio 1 serão geradas duas instancias pyspark que irão executar o mesmo arquivo desafio1_1.py para gerar dois arquivos CSVs diferentes.
- No desafio 2, além de só propor a query para resolução do problema, criei o arquivo gerartabelas.py para gerar duas tabelas com dados ficticios, o arquivo load.py para subir os dados no banco postgres  que é acessado através do pdadmin no endereço http://localhost:8082/, o login é admin@admin.com e a senha é admin, após isso clicar em "new server" no postgres e configurar conforme os dados do docker-compose, e finalmente o arquivo solucao2.py onde executo a query via python e gero um arquivo csv com o resultado.
- No desafio 3, os tratamentos conforme solicitado, usando apenas python.