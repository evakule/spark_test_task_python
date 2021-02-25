from pyspark.sql import SparkSession
from suicide_statistic_calculation import SuicideStatisticCalculation

session = SparkSession.builder.appName("humanlves-pyspark").getOrCreate()
data = session.read.csv(path='master.csv', sep=',', inferSchema=True, header=True)

calculation = SuicideStatisticCalculation(data)


print(calculation.get_top_five(data))


calculation.get_avg_suicide_by_one_hundred(data).foreach(lambda x: print(x))


calculation.get_country_age_distribution(data).foreach(lambda x: print(x))


calculation.get_country_year_to_suicide_amount_to_gdp(data).foreach(lambda x: print(x))
