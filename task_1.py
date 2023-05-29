from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Выберите 15 стран с наибольшим процентом переболевших на 31 марта(в выходящем датасете необходимы колонки:
# iso_code, страна, процент переболевших)


spark = SparkSession.builder.master('local').appName('Task 1').getOrCreate()

df = spark.read.csv('owid-covid-data.csv', header=True, inferSchema=True)

df.filter(df['date'] == '2021-03-31').select('iso_code', 'location',
                                             F.round((F.col('total_cases') / F.col('population') * 100), 2).
                                             alias('percent')).\
                                             orderBy(F.col('percent').desc()).show(15)
