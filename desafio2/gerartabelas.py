import pandas as pd
import random
from datetime import datetime, timedelta

# Função para gerar dados fictícios
def generate_data(num_rows):
    eventos = ['Pedido Criado', 'Pedido Enviado', 'Pedido Entregue', 'Pedido Cancelado']
    
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 1, 1)
    
    data_pedido = []
    id_pedido = []
    valor_pedido = []
    
    # Gerar dados para a Tabela_A (Pedidos)
    for i in range(num_rows):
        id_pedido.append(i + 1)
        data_pedido.append(start_date + timedelta(days=random.randint(0, (end_date - start_date).days)))
        valor_pedido.append(round(random.uniform(50.00, 5000.00), 2))
    
    # Gerar dados para a Tabela_B (Eventos dos Pedidos)
    data_evento = []
    evento = []
    id_pedido_evento = []
    
    for i in range(num_rows):
        num_eventos = random.randint(1, 5)  
        for _ in range(num_eventos):
            id_pedido_evento.append(i + 1)
            data_evento.append(data_pedido[i] + timedelta(days=random.randint(1, 30)))  # Eventos após o pedido
            evento.append(random.choice(eventos))
    
    tabela_a = pd.DataFrame({
        'id_pedido': id_pedido,
        'data_pedido': data_pedido,
        'valor_pedido': valor_pedido
    })
    
    tabela_b = pd.DataFrame({
        'id_pedido': id_pedido_evento,
        'data_evento': data_evento,
        'evento': evento
    })
    
    return tabela_a, tabela_b

# Gerar dados fictícios para as duas tabelas
tabela_a, tabela_b = generate_data(100)  # Gerando 100 pedidos

tabela_a.to_csv('tabela_a.csv', index=False)
tabela_b.to_csv('tabela_b.csv', index=False)

print("Arquivos CSV gerados: tabela_a.csv e tabela_b.csv")
