from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# Посчитайте изменение случаев относительно предыдущего дня в России за последнюю неделю марта 2021.
# (например: в россии вчера было 9150, сегодня 8763, итог: -387)
# (в выходящем датасете необходимы колонки: число, кол-во новых случаев вчера, кол-во новых случаев сегодня, дельта)

spark = SparkSession.builder.master('local').appName('Task 3').getOrCreate()

df = spark.read.csv('owid-covid-data.csv', header=True, inferSchema=True)

w = Window.partitionBy('iso_code').orderBy('date')

df.filter((df['date'].between('2021-03-24', '2021-03-31')) & (df['location'].startswith('Russ'))).\
    withColumn('previous_date_cases', F.lag('new_cases', default=0).over(w)).\
    select('date', 'previous_date_cases', 'new_cases', (F.col('new_cases') - F.col('previous_date_cases')).\
           alias('delta')).filter(df['date'].between('2021-03-25', '2021-03-31')).show(10)




