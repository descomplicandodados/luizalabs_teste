from sqlalchemy import create_engine
import pandas as pd

# Configurações de conexão
DATABASE_URL = "postgresql://admin:admin@db:5432/mydatabase"

engine = create_engine(DATABASE_URL)

query = """
    SELECT
        a.id_pedido,
        a.data_pedido,
        a.valor_pedido,
        b.evento AS ultimo_evento,
        b.data_evento
    FROM
        tabela_a a
    JOIN (
        SELECT
            id_pedido,
            evento,
            data_evento,
            ROW_NUMBER() OVER (PARTITION BY id_pedido ORDER BY data_evento DESC) AS rn
        FROM
            tabela_b
    ) b ON a.id_pedido = b.id_pedido AND b.rn = 1
    ORDER BY
        a.id_pedido;
"""

# Executar a consulta e salvar os resultados em um arquivo CSV
try:
    with engine.connect() as connection:
        results = pd.read_sql(query, connection)
        results.to_csv("resultado_pedidos.csv", index=False)
        print("Resultados salvos em resultado_pedidos.csv")

except Exception as e:
    print(f"Erro ao executar a consulta: {e}")
