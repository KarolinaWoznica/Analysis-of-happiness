# Databricks notebook source
# MAGIC %md
# MAGIC # Analiza szczęścia w różnych krajach
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Dane zostały pobrane ze strony kaggle : https://www.kaggle.com/datasets/yamaerenay/world-happiness-report-preprocessed?select=2020_report.csv
# MAGIC
# MAGIC Dane użyte do raportu to:
# MAGIC - kraje
# MAGIC - wskaźnik szczęścia
# MAGIC - PKB na mieszkańca
# MAGIC - wskaźnik pomocy socjalnej
# MAGIC - wskaźnik zdrowia
# MAGIC - wskaźnik wolności
# MAGIC - wskaźnik hojności
# MAGIC - wskaźnik zaufania do rządu
# MAGIC - kontynet
# MAGIC - pozostałości dystopii czyli najniższy możliwy wskaźnik szczęśćia w danym kraju

# COMMAND ----------

# MAGIC %md
# MAGIC #Opis projektu
# MAGIC
# MAGIC Szczęście jest pojęciem psychologicznym i filozoficznym, przez każdego interpretowane w indywidualny sposób. Szczęście może wpływać pozytywnie na zdrowie, zmniejszać poziom stresu, zwiększać produktywność czy wpływać pozytywnie na koncentracje i kreatywność.
# MAGIC
# MAGIC W tym raporcie zostanie przeanalizowane czy szczęście w danym kraju zależy od czynników takich jak PKB na mieszkańca, zdrowie, zaufanie do rządu, pomoc socjalna, wolność oraz hojność.
# MAGIC
# MAGIC Badania do tego raportu zostały przeprowadzone w 138 krajach w roku 2020.

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import *

d = spark.read.format('csv').options(header = 'true', inferSchema = 'true', delimiter = ',').load('/FileStore/tables/2020_report.csv')

display(d)

# COMMAND ----------

display(d.count())

# COMMAND ----------

d.groupBy("continent").count().show()

# COMMAND ----------

dh = d
dh.createOrReplaceTempView("hive_database")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_database

# COMMAND ----------

# MAGIC %sql
# MAGIC select country, happiness_score
# MAGIC from hive_database
# MAGIC order by happiness_score;

# COMMAND ----------

# MAGIC %md
# MAGIC Z tabeli powyżej można przeczytać, że najmniej szczęśliwe kraje to Afganistan, następnie Zimbabwe i Rwanda z wynikami odpowiednio 2.56, 3.30 i 3.31, natomiast najszczęśliwszy kraj to Finlandia, a następnie Dania i Szwajcaria z wynikami odpowiednio 7.81, 7.65 i 7.55. Porównując te dane widać, że różnica pomiędzy najszczęśliwszym krajem a najmniej szczęśliwym wynosi ponad 5 punktów.

# COMMAND ----------

# MAGIC %sql
# MAGIC select country, round(happiness_score, 3)
# MAGIC from hive_database
# MAGIC order by happiness_score;

# COMMAND ----------

# MAGIC %md
# MAGIC #Podział na kontynenty

# COMMAND ----------

# MAGIC %sql
# MAGIC select continent, round(avg(happiness_score), 3)
# MAGIC from hive_database
# MAGIC group by continent
# MAGIC order by round(avg(happiness_score), 3);

# COMMAND ----------

# MAGIC %md
# MAGIC Z tego wykresu można odczytać, że najszczęśliwszy kontynent to Australia, następnie Ameryka Północna oraz Europa. Pozostałe trzy kontynenty średnio przyjęły wartości poniżej 6. Może być to związane z podziałem gospodarczym na bogatą Północ i biedne Południe.

# COMMAND ----------

# MAGIC %sql
# MAGIC select continent, round(avg(happiness_score), 3), round(avg(gdp_per_capita), 3)
# MAGIC from hive_database
# MAGIC group by continent
# MAGIC order by round(avg(happiness_score), 3);

# COMMAND ----------

# MAGIC %md
# MAGIC Ten wykres przedstawia zależność pomiędzy średnim PKB na mieszkańca na danych kontynentach, co potwierdza tezę powyżej, że szczęście może zależeć od PKB na mieszkańca oraz że kraje bogatej Północy są szczęśliwsze.

# COMMAND ----------

# MAGIC %sql
# MAGIC select continent, round(avg(happiness_score), 3), round(avg(freedom), 3), round(avg(health), 3), round(avg(social_support), 3), round(avg(generosity), 3)
# MAGIC from hive_database
# MAGIC group by continent
# MAGIC order by round(avg(happiness_score), 3);

# COMMAND ----------

# MAGIC %md
# MAGIC Z tej tabeli można wywnioskować, że kontynenty z wyższym średnim wskaźnikiem szczęścia, również mają wyższe średnie wskaźniki wolności, zdrowia, hojności oraz pomocy społecznej. Potwierdza to tezę, że szczęścia zależy od tych czynników.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT continent, min(dystopia_residual), max(dystopia_residual), round(avg(happiness_score), 3)
# MAGIC FROM hive_database
# MAGIC group by continent;

# COMMAND ----------

# MAGIC %md
# MAGIC Tabela ta obrazuje wskaźnik pozostałości dystopii, czyli najniższy możliwy wskaźnik szczęścia na danym kontynencie.

# COMMAND ----------

d_rounded = d.select(f.round("happiness_score", 3).alias("happiness_score"), "country", "continent", f.round("gdp_per_capita", 3).alias("gdp_per_capita"), f.round("social_support", 3).alias("social_support"), f.round("health", 3).alias("health"), f.round("freedom", 3).alias("freedom"), f.round("government_trust", 3).alias("government_trust"), f.round("dystopia_residual", 3).alias("dystopia_residual"), f.round("generosity", 3).alias("generosity"))

# COMMAND ----------

d_rounded.select("country", d_rounded["happiness_score"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Podział na trzy grupy krajów ze względu na poziom szczęścia

# COMMAND ----------


d1 = d.withColumn("Poziom szczęscia w krajach", 
                 f.when((d.happiness_score >= 2) & (d.happiness_score < 5), "Najmniej szczęśliwe kraje")\
                 .when((d.happiness_score >= 5) & (d.happiness_score < 7), "Średnio szczęśliwe kraje")\
                 .when((d.happiness_score >= 7), "Najbardziej szczęśliwe kraje"))
d3 = d1.groupBy("Poziom szczęscia w krajach").count()
d3.show(3, False)
display(d3)

# COMMAND ----------

# MAGIC %md
# MAGIC Zbiór 138 krajów został podzielony na trzy kategorie ze względu na poziom szczęścia: do 5, od 5 do 7 i od 7. Jak widać najwięcej krajów, to kraje średnio szczęśliwe. Grupa ta wynosi 77 krajów.

# COMMAND ----------

d3 = d1.groupBy("Poziom szczęscia w krajach").agg(f.round(f.avg("social_support"), 3).alias("social_support"), f.count("*").alias("country"))
d3.show(3, False)
display(d3)

# COMMAND ----------

d3 = d1.groupBy("Poziom szczęscia w krajach").agg(f.round(f.avg("gdp_per_capita"), 3).alias("gdp_per_capita"), f.count("*").alias("country"))
d3.show(3, False)
display(d3)

# COMMAND ----------

d3 = d1.groupBy("Poziom szczęscia w krajach").agg(f.round(f.avg("health"), 3).alias("health"), f.count("*").alias("country"))
d3.show(3, False)
display(d3)

# COMMAND ----------

d3 = d1.groupBy("Poziom szczęscia w krajach").agg(f.round(f.avg("freedom"), 3).alias("freedom"), f.count("*").alias("country"))
d3.show(3, False)
display(d3)

# COMMAND ----------

d3 = d1.groupBy("Poziom szczęscia w krajach").agg(f.round(f.avg("generosity"), 3).alias("generosity"), f.count("*").alias("country"))
d3.show(3, False)
display(d3)

# COMMAND ----------

d3 = d1.groupBy("Poziom szczęscia w krajach").agg(f.round(f.avg("government_trust"), 3).alias("government_trust"), f.count("*").alias("country"))
d3.show(3, False)
display(d3)


# COMMAND ----------

# MAGIC %md
# MAGIC Na wszystkich powyższych wykresach pokazujących podział krajów na trzy grupy ze względu na poziom szczęścia. Widać, że kraje z najwyższym poziomem szczęścia, mają najwyższe wszystkie wskaźniki. Natomiast kraje z najniższym poziomem trzecim mają najniższe wskaźniki dla wsparcia socjalnego, PKB na mieszkańca, zdrowie, wolność. Jednak dla wskaźników zaufania do rządu oraz hojności przyjmują one wyższe wartości niż dla krajów ze średnim poziomem szczęścia, chociaż w odniesieniu do wskaźnika zaufania do rządu różnica jest niewielka.

# COMMAND ----------

# MAGIC %md
# MAGIC # Podział 10 najszczęśliwszych krajów i 10 najmniej szczęśliwych krajów

# COMMAND ----------

min_score = d_rounded.agg(f.min("happiness_score")).collect()[0][0]
max_score = d_rounded.agg(f.max("happiness_score")).collect()[0][0]

min_country = d_rounded.filter(d_rounded.happiness_score == min_score).select("country").collect()[0][0]
max_country = d_rounded.filter(d_rounded.happiness_score == max_score).select("country").collect()[0][0]


results = spark.createDataFrame([("Minimum", min_score, min_country),
                                  ("Maximum", max_score, max_country)],
                                 ["Type", "Score", "Country"])
display(results)

# COMMAND ----------

d_sorted1 = d_rounded.sort(d_rounded.happiness_score.asc())
d4 = d_sorted1.limit(10)
display(d4.select("happiness_score", "country", "continent"))

d_sorted2 = d_rounded.sort(d_rounded.happiness_score.desc())
d5 = d_sorted2.limit(10)
display(d5.select("happiness_score", "country", "continent"))



# COMMAND ----------

# MAGIC %md
# MAGIC Z dwóch powyższych wykresów można odczytać, że 10 najmniej szczęśliwych krajów to w większości kraje Afryki, trzy z nich są z Azji oraz jeden z Ameryki Południowej. Natomiast 9 krajów z 10 najszczęśliwszych to kraje europejskie oraz jeden z Australii, czyli Nowa Zelandia. To również potwierdza, że kraje bogatej Północy mają wyższy wskaźniki poziomu szczęścia niż biednego Południa.

# COMMAND ----------

display(d4.select("happiness_score", "country", "gdp_per_capita", "government_trust"))
display(d5.select("happiness_score", "country", "gdp_per_capita", "government_trust"))

# COMMAND ----------

display(d4.select("happiness_score", "country", "social_support", "freedom", "health", "generosity"))
display(d5.select("happiness_score", "country", "social_support", "freedom", "health", "generosity"))

# COMMAND ----------

# MAGIC %md
# MAGIC Ostatnie cztery tabele przedstawiają, poziomy różnych wskaźników wpływających na wskaźnik szczęścia w 10 krajach z najwyższym wskaźnikiem szczęścia oraz w 10 krajach z najniższym wskaźnikiem szczęścia. Największa różnica jest widoczna przy wskaźnikach PKB na mieszkańca oraz zdrowia, które są dużo wyższy dla krajów najszczęśliwszych. Również dla krajów najszczęśliwszych wskaźnik pomocy socjalnej jest wyższy. Wskaźniki wolności i hojności są na podobnym poziomie dla obu grup, jednak pomimo tego wyższe wskaźniki przyjmuje w większości w krajach najszczęśliwszych. Jeśli weźmie się pod uwagę wskaźnik zaufania do rządu, to oprócz Islandii gdzie ten wskaźnik jest niższy, to w pozostałych krajach z najwyższym poziomem szczęścia jest on wyższy w porównaniu do krajów z niższym poziomem szczęścia (oprócz Rwandy gdzie jest on na wysokim poziomie).

# COMMAND ----------

# MAGIC %md
# MAGIC # Podsumowanie
# MAGIC Biorąc pod uwagę, powyższe dane można zauważyć zależność pomiędzy poziomem szczęścia a resztą wskaźników wykorzystanych w trakcie analizy. Oznacza to, że im wyższy poziom wskaźników PKB na mieszkańca, zdrowia (gdzie zależność przy tych dwóch wskaźnikach była najbardziej widoczna), wolności, hojności, zaufania do rządu, pomocy socjalnej, tym wyższy poziom szczęścia.
# MAGIC
# MAGIC Najlepiej tę zależność można zauważyć przy podziale krajów ze względu na kontynent. Gdzie wskaźniki wykazują liniową zależność względem wskaźnika szczęścia. Gdy wskaźniki rosną, to również poziom szczęścia rośnie. Również te dane wskazują, że kraje bogatej Północy, czyli kraje lepiej rozwinięte, zasobne, niemające problemów skrajnego ubóstwa czy głodu, są krajami szczęśliwszymi niż kraje biednego Południa, gdzie te problemy występują. Co oczywiście jest zrozumiałe i było do przewidzenia.
# MAGIC
# MAGIC W podziale na trzy grupy ze względu na poziom szczęścia. Grupa średnio szczęśliwych jest najliczniejsza. Krajów najszczęśliwszych jest najmniej, jednak to właśnie dla nich wszystkie wskaźniki porównujące były najwyższe. Jedynie dla najmniej szczęśliwych krajów dwa wskaźniki były na średnim poziomie, chociaż z niewielką różnicą.
# MAGIC
# MAGIC Ostatni podział obrazuje zależność między wskaźnikami a poziomem szczęścia indywidualnie dla każdego kraju (10 najszczęśliwszych krajów i 10 najmniej szczęśliwych). Tutaj również można wywnioskować, że im wyższe są wskaźniki, tym poziom szczęścia jest wyższy, choć zależność ta dla niektórych wskaźników już w tym wypadku nie była, aż tak widoczna.
# MAGIC
# MAGIC Podsumowując, szczęście w danym kraju zależy od czynników takich jak PKB na mieszkańca, zdrowie, zaufanie do rządu, pomoc socjalna, wolność oraz hojność. Również położenie geograficzne ma znaczenie ze względu na dostęp do zasobów oraz rozwoju gospodarczego.
