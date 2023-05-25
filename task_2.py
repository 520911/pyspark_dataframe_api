from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Top 10 стран с максимальным зафиксированным кол - вом новых случаев за последнюю неделю марта 2021
# в отсортированном порядке по убыванию (в выходящем датасете необходимы колонки: число, страна, кол-во новых случаев)

spark = SparkSession.builder.master('local').appName('Task 2').getOrCreate()

df = spark.read.csv('owid-covid-data.csv', header=True, inferSchema=True)

df_grouped = df.filter(df['date'].between('2021-03-25', '2021-03-31')).where(~F.col('iso_code').contains('OWID')).\
    groupBy('location').agg(F.max('new_cases').alias('cases'))

df['date', 'new_cases'].join(df_grouped, df['new_cases'] == df_grouped['cases'], how='inner').select('date', 'location', 'cases').orderBy(F.col('cases').desc()).show(10)


