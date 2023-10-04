# eng_dados-data_extract
Projeto final proposto no módulo IV (Extração de Dados) da trilha de Engenharia de Dados (Santander Coders 2023)


Sistema de Monitoramento de Avanços no Campo da Genômica

Você trabalha no time de engenharia de dados na HealthGen, uma empresa especializada em genômica e pesquisa de medicina personalizada. A genômica é o estudo do conjunto completo de genes de um organismo, desempenha um papel fundamental na medicina personalizada e na pesquisa biomédica. Permite a análise do DNA para identificar variantes genéticas e mutações associadas a doenças e facilita a personalização de tratamentos com base nas características genéticas individuais dos pacientes.

A empresa precisa se manter atualizada sobre os avanços mais recentes na genômica, identificar oportunidades para pesquisa e desenvolvimento de tratamentos personalizados e acompanhar as tendências em genômica que podem influenciar estratégias de pesquisa e desenvolvimento. Pensando nisso, o time de dados apresentou uma proposta de desenvolvimento de um sistema que coleta, analisa e apresenta as últimas notícias relacionadas à genômica e à medicina personalizada, e também estuda o avanço do campo nos últimos anos.

O time de engenharia de dados tem como objetivo desenvolver e garantir um pipeline de dados confiável e estável. As principais atividades são:

1. Consumo de dados com a News API:
Implementar um mecanismo para consumir dados de notícias de fontes confiáveis e especializadas em genômica e medicina personalizada, a partir da News API:
https://newsapi.org/

2. Definir Critérios de Relevância:
Desenvolver critérios precisos de relevância para filtrar as notícias. Por exemplo, o time pode se concentrar em notícias que mencionem avanços em sequenciamento de DNA, terapias genéticas personalizadas ou descobertas relacionadas a doenças genéticas específicas.

3. Cargas em Batches:
Armazenar as notícias relevantes em um formato estruturado e facilmente acessível para consultas e análises posteriores. Essa carga deve acontecer uma vez por hora. Se as notícias extraídas já tiverem sidos armazenadas na carga anterior, o processo deve ignorar e não armazenar as notícias novamente, os dados carregados não podem ficar duplicados.

4. Dados transformados para consulta do público final
A partir dos dados carregados, aplicar as seguintes transformações e armazenar o resultado final para a consulta do público final:

* 4.1 - Quantidade de notícias por ano, mês e dia de publicação;
* 4.2 - Quantidade de notícias por fonte e autor;
* 4.3 - Quantidade de aparições de três palavras chaves por ano, mês e dia de publicação (as três palavras chaves serão as mesmas usadas para fazer os filtros de relevância do item 2).

Atualizar os dados transformados uma vez por dia.

Além das atividades principais, existe a necessidade de busca de dados por eventos em tempo real quando é necessário, para isso foi desenhado duas opções:

Opção 1 - Apache Kafka e Spark Streaming:
Preparar um pipeline com Apache Kafka e Spark Streaming para receber os dados do Produtor Kafka representado por um evento manual e consumir os dados com o Spark Streaming armazenando os resultados temporariamente. Em um processo paralelo, verificar os resultados armazenados temporiamente e armazenar no mesmo destino do item 3 (3. Cargas em Batches) aqueles resultados que ainda não foram armazenados no destino (os dados carregados não podem ficar duplicados). E por fim, eliminar os dados temporários após a verificação e a eventual carga.

Opção 2 - Webhooks com notificações por eventos:
Configurar um webhook para adquirir as últimas notícias a partir de um evento representado por uma requisição POST e fazer a chamada da API e por fim armazenar os resultados temporariamente. Em um processo paralelo, verificar os resultados armazenados temporiamente e armazenar no mesmo destino do item 3 (3. Cargas em Batches) aqueles resultados que ainda não foram armazenados no destino (os dados carregados não podem ficar duplicados). E por fim, eliminar os dados temporários após a verificação e a eventual carga.

