from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Cria uma Spark session para o primeiro processo
spark1 = SparkSession.builder \
    .appName("Desafio 1.1 - Sem Filial") \
    .getOrCreate()

# Carrega o arquivo CSV
df1 = spark1.read.csv('/app/tab_venda_servicos.csv', header=True, inferSchema=True)

# Converte a coluna de data para o formato de data
df1 = df1.withColumn('data_venda', F.to_date(df1['data_venda'], 'yyyy-MM-dd'))

# Extrai mês e ano da data
df1 = df1.withColumn('ano_mes', F.date_format(df1['data_venda'], 'yyyy-MM'))

# Define o tipo de serviço
df1 = df1.withColumn('tipo_servico', F.when(df1['servico'].isin(5, 6, 7, 8), 'garantia estendida')
                              .when(df1['servico'].isin(19, 20), 'seguro roubo e furto')
                              .otherwise('outro'))

# Agrupa os dados por ano/mês e tipo de serviço, e soma o valor total
resultado_mensal1 = df1.groupBy('ano_mes', 'tipo_servico') \
    .agg(F.sum('valor_total').alias('valor_venda_mensal'))

# Cria uma janela para calcular o valor acumulado
window_spec1 = Window.partitionBy('tipo_servico').orderBy('ano_mes').rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Adiciona a coluna de valor acumulado
resultado_acumulado1 = resultado_mensal1.withColumn('valor_acumulado', F.sum('valor_venda_mensal').over(window_spec1))

# Salva o resultado acumulado em um arquivo CSV (desafio1_1.csv)
resultado_acumulado1.write.csv('/app/desafio1_1', header=True)

# Para encerrar a primeira Spark session
spark1.stop()

# Cria uma Spark session para o segundo processo
spark2 = SparkSession.builder \
    .appName("Desafio 1.2 - Com Filial") \
    .getOrCreate()

# Carrega o arquivo CSV
df2 = spark2.read.csv('/app/tab_venda_servicos.csv', header=True, inferSchema=True)

# Converte a coluna de data para o formato de data
df2 = df2.withColumn('data_venda', F.to_date(df2['data_venda'], 'yyyy-MM-dd'))

# Extrai mês e ano da data
df2 = df2.withColumn('ano_mes', F.date_format(df2['data_venda'], 'yyyy-MM'))

# Define o tipo de serviço
df2 = df2.withColumn('tipo_servico', F.when(df2['servico'].isin(5, 6, 7, 8), 'garantia estendida')
                              .when(df2['servico'].isin(19, 20), 'seguro roubo e furto')
                              .otherwise('outro'))

# Agrupa os dados por ano/mês, filial e tipo de serviço, e soma o valor total
resultado_mensal2 = df2.groupBy('ano_mes', 'filial_venda', 'tipo_servico') \
    .agg(F.sum('valor_total').alias('valor_venda_mensal'))

# Cria uma janela para calcular o valor acumulado
window_spec2 = Window.partitionBy('filial_venda', 'tipo_servico').orderBy('ano_mes').rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Adiciona a coluna de valor acumulado
resultado_acumulado2 = resultado_mensal2.withColumn('valor_acumulado', F.sum('valor_venda_mensal').over(window_spec2))

# Cria uma janela para calcular o valor do mês anterior
window_prev_month2 = Window.partitionBy('filial_venda', 'tipo_servico').orderBy('ano_mes')

# Adiciona a coluna de valor do mês anterior
resultado_acumulado2 = resultado_acumulado2.withColumn('valor_venda_mes_anterior', F.lag('valor_venda_mensal').over(window_prev_month2))

# Calcula o percentual de crescimento
resultado_acumulado2 = resultado_acumulado2.withColumn('percentual_crescimento', 
    (F.col('valor_venda_mensal') - F.col('valor_venda_mes_anterior')) / F.col('valor_venda_mes_anterior') * 100)

# Filtra os meses com percentual de crescimento válido
resultado_crescimento2 = resultado_acumulado2.filter(F.col('valor_venda_mes_anterior').isNotNull())

# Ordena por percentual de crescimento e pega os 5 meses com maior crescimento
resultado_top5_crescimento2 = resultado_crescimento2.withColumn('rank', F.rank().over(Window.partitionBy('filial_venda').orderBy(F.desc('percentual_crescimento')))) \
    .filter(F.col('rank') <= 5) \
    .select('ano_mes', 'filial_venda', 'percentual_crescimento', 'valor_venda_mensal')

# Salva o resultado em um arquivo CSV (desafio1_2.csv)
resultado_top5_crescimento2.write.csv('/app/desafio1_2', header=True)

# Para encerrar a segunda Spark session
spark2.stop()