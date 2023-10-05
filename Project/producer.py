# Databricks notebook source

# COMMAND ----------

# MAGIC %pip install kafka-python

# COMMAND ----------

import requests
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer

class NewsIngest():

    def __init__(self, brokers: list):
        self.brokers = brokers
        self.kafka_producer = KafkaProducer(bootstrap_servers=brokers)

    def get_data(self):
        api_key  = '44fdc23c5c7a4469b609578563c10169'
        url      = 'https://newsapi.org/v2/everything?q=genomics&apiKey='
        response = requests.get(url + api_key)
        return response.json()  # Chame a função para obter o resultado como um dicionário

    def publish_news(self):
        noticias = self.get_data()

        for noticia in noticias['articles']:  # Altere de 'noticias' para 'articles' para acessar as notícias
            print(f"Enviando noticia: '{noticia['title']}' para o tópico_noticias")
            self.kafka_producer.send("topico_noticias", json.dumps(noticia).encode("utf-8"))

        print("Envio do lote de produtos finalizado com sucesso")

pipeline = NewsIngest(brokers=["localhost:9092"])
pipeline.publish_news()
