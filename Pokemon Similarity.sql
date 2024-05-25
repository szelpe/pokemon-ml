-- Databricks notebook source
SELECT * FROM csv.`/Volumes/main/pokemon/data/pokemon.csv`

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW pokemon_raw
USING csv 
OPTIONS (
  path '/Volumes/main/pokemon/data/pokemon.csv',
  header true
);

SELECT * FROM pokemon_raw

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.functions import collect_list
-- MAGIC
-- MAGIC df = spark.read.csv('/Volumes/main/pokemon/data/pokemon.csv', header='true')
-- MAGIC df_species = spark.read.csv('/Volumes/main/pokemon/data/pokemon_species.csv', header='true')
-- MAGIC df_text = spark.read.csv('/Volumes/main/pokemon/data/pokemon_species_flavor_text.csv', header='true', multiLine=True)
-- MAGIC
-- MAGIC df_text_array = df_text.groupBy("species_id").agg(collect_list("flavor_text").alias("flavor_text_array"))
-- MAGIC
-- MAGIC display(df
-- MAGIC         .join(df_species, df_species.id == df.species_id)
-- MAGIC         .join(df_text_array, df_text_array.species_id == df.species_id)
-- MAGIC )
