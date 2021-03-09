from pyspark.sql import SparkSession
from suicide_statistic_calculation import SuicideStatisticCalculation

spark = SparkSession.builder.appName("humanlves-pyspark").getOrCreate()
data = spark.read.csv(path='master.csv', sep=',', inferSchema=True, header=True)

calculation = SuicideStatisticCalculation(data)


print(calculation.get_top_five())


calculation.get_avg_suicide_by_one_hundred().foreach(lambda x: print(x))


calculation.get_country_age_distribution().foreach(lambda x: print(x))


calculation.get_country_year_to_suicide_amount_to_gdp().foreach(lambda x: print(x))

df = spark.createDataFrame(calculation.get_avg_suicide_by_one_hundred())
df.write.csv('output_package', header=True)