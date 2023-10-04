# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Realizando Download Apache Kafka

# COMMAND ----------

# MAGIC %sh 
# MAGIC sudo wget https://downloads.apache.org/kafka/3.5.1/kafka_2.12-3.5.1.tgz

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Descompactando arquivos

# COMMAND ----------

# MAGIC %sh 
# MAGIC tar -xvf kafka_2.12-3.5.1.tgz