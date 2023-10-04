# Databricks notebook source
from pyspark.sql import SparkSession

spark = (
    SparkSession
        .builder
        .appName("HelloWorldStreaming")
        .getOrCreate()
)

# COMMAND ----------

df_noticias = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "topico_noticias")
)

df_noticias

# COMMAND ----------

df_noticias = df_noticias.load()

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, MapType, BinaryType, LongType, TimestampType

# COMMAND ----------

df_noticias = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "topico_noticias")
        .load()
)

message_schema = StructType(
    [
        StructField("source", MapType(StringType(), StringType())),
        StructField("author", StringType()),
        StructField("title", StringType()),
        StructField("description", StringType()),
        StructField("url", StringType()),
        StructField("urlToImage", StringType()),
        StructField("publishedAt", TimestampType()),
        StructField("content", StringType()),
    ]
)

df_noticias_novas = df_noticias.withColumn("value", from_json(col("value").cast("STRING"), message_schema))

df_noticias_novas = df_noticias_novas.selectExpr("value.*")

# COMMAND ----------

query = (
    df_noticias_novas.writeStream
        .format("console")
        .outputMode("append")
        .start()
)

# COMMAND ----------

df_noticias_novas.writeStream.format("parquet").option("checkpointLocation", "dbfs:/tmp/checkpoint/prj").option("path", "dbfs:/tmp/output/Project/").start()

# COMMAND ----------

spark.read.parquet("dbfs:/tmp/output/Project/").display()

# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Carregando os dados a partir do arquivo Parquet
transform = spark.read.parquet("dbfs:/tmp/output/Project/")

# 4.1 - Quantidade de notícias por ano, mês e dia de publicação
transform = transform.withColumn("data_publicacao", F.to_date("publishedAt"))  # Converte a coluna 'publishedAt' para o tipo de data
quantidade_por_data = transform.groupBy(F.year("data_publicacao").alias("ano"), F.month("data_publicacao").alias("mes"), F.dayofmonth("data_publicacao").alias("dia")).count()

# 4.2 - Quantidade de notícias por fonte e autor
quantidade_por_fonte_autor = transform.groupBy("source.name", "author").count()

# 4.3 - Quantidade de aparições de 3 palavras-chave por ano, mês e dia de publicação
palavras_chave = ["genomics", "dna", "genes"]
for palavra in palavras_chave:
    transform = transform.withColumn(palavra, F.when(F.col("content").contains(palavra), 1).otherwise(0))

quantidade_palavras_chave = transform.groupBy(F.year("data_publicacao").alias("ano"), F.month("data_publicacao").alias("mes"), F.dayofmonth("data_publicacao").alias("dia")).agg(*[F.sum(palavra).alias(palavra) for palavra in palavras_chave])

# Caminhos para salvar os arquivos Parquet
caminho_quantidade_por_data = "dbfs:/tmp/output/quantidade_por_data.parquet"
caminho_quantidade_por_fonte_autor = "dbfs:/tmp/output/quantidade_por_fonte_autor.parquet"
caminho_quantidade_palavras_chave = "dbfs:/tmp/output/quantidade_palavras_chave.parquet"

# Salvando os DataFrames com a opção de sobrescrever (overwrite) ativada
quantidade_por_data.write.mode("overwrite").parquet(caminho_quantidade_por_data)
quantidade_por_fonte_autor.write.mode("overwrite").parquet(caminho_quantidade_por_fonte_autor)
quantidade_palavras_chave.write.mode("overwrite").parquet(caminho_quantidade_palavras_chave)


# COMMAND ----------

quantidade_por_data.display()
quantidade_por_fonte_autor.display()
quantidade_palavras_chave.display()