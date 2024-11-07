import json
import pandas as pd
from collections import defaultdict
from datetime import datetime

# Função para calcular o rendimento
def aplicar_rendimento(saldo):
    if saldo > 0:
        return round(saldo * (1 + 0.00015), 2)  # Rendimento de 0,015% com arredondamento
    return round(saldo, 2)

# Função para processar o arquivo JSON
def processar_transacoes(filename):
    # Definir o intervalo de datas
    data_inicio = datetime.strptime('2022-07-01', '%Y-%m-%d').date()
    data_fim = datetime.strptime('2022-12-31', '%Y-%m-%d').date()
    
    with open(filename, 'r') as file:
        dados = json.load(file)

    # Dicionário para armazenar os dados por conta e data
    resultados = defaultdict(lambda: defaultdict(lambda: {
        'entrada': 0.0, 'saida': 0.0, 'saldo_diario': 0.0, 'saldo_com_rendimento': 0.0
    }))

    # Processando cada conta e suas transações
    for conta_data in dados:
        conta = conta_data['conta']
        transacoes = conta_data['transacoes']
        
        saldo_acumulado = 0.0  # Saldo acumulado ao longo dos dias

        for transacao in transacoes:
            data_transacao = transacao['data']
            tipo = transacao['tipo']
            valor = transacao['valor']

            data = datetime.strptime(data_transacao, "%Y-%m-%d %H:%M:%S").date()
            
            # Filtrar as transações no intervalo de datas
            if data_inicio <= data <= data_fim:
                if tipo == 'entrada':
                    resultados[conta][data]['entrada'] += valor
                elif tipo == 'saida':
                    resultados[conta][data]['saida'] += valor

    # Calcular o saldo diário e aplicar o rendimento
    linhas = []
    for conta, dias in resultados.items():
        saldo_acumulado = 0.0
        for data, transacao in sorted(dias.items()):
            entrada = round(transacao['entrada'], 2)  # Arredondar para 2 casas decimais
            saida = round(transacao['saida'], 2)      # Arredondar para 2 casas decimais
            
            # Cálculo do saldo diário
            saldo_diario = round(saldo_acumulado + entrada - saida, 2)
            saldo_acumulado = saldo_diario
            
            # Aplicar rendimento se o saldo for positivo
            saldo_com_rendimento = aplicar_rendimento(saldo_diario)
            
            linhas.append({
                'Conta': conta,
                'Data': data,
                'Valor de Entrada': entrada,
                'Valor de Saída': saida,
                'Saldo Diário': saldo_diario,
                'Saldo Diário com Rendimento': saldo_com_rendimento
            })
    
    df = pd.DataFrame(linhas)
    df.to_csv('desafio3.csv', index=False, sep=';')

processar_transacoes('json_transacoes_contas.txt')
