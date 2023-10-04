# Databricks notebook source
# MAGIC %md
# MAGIC ## Criação de um Tópico

# COMMAND ----------

ls -la kafka_2.12-3.5.1/bin/

# COMMAND ----------

# MAGIC %sh
# MAGIC ./kafka_2.12-3.5.1/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic topico_noticias --partitions 1 --replication-factor 1

# COMMAND ----------

# MAGIC %sh
# MAGIC ./kafka_2.12-3.5.1/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list