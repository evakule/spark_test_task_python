class SuicideStatisticCalculation:
    rdd = None

    def __init__(self, data):
        self.rdd = data.rdd

    def get_top_five(self):
        return self.rdd.map(lambda line: (line.country_year, line.suicides_no)) \
            .reduceByKey(lambda x, y: x + y) \
            .map(lambda x: (x[1], x[0])) \
            .sortByKey(ascending=False) \
            .map(lambda x: (x[1], x[0])).take(5)

    def get_avg_suicide_by_one_hundred(self):
        return self.rdd.map(lambda line: (line.country_year, (1, line.suicides_100k_pop))) \
            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
            .map(lambda x: (x[0], x[1][1] / x[1][0])) \
            .map(lambda x: (x[1], x[0])) \
            .sortByKey(ascending=False) \
            .map(lambda x: (x[1], x[0]))

    def get_country_age_distribution(self):
        return self.rdd.map(lambda x: (x.country + ': ' + x.age + ' = ', x.suicides_no)) \
            .reduceByKey(lambda x, y: x + y)

    def get_country_year_to_suicide_amount_to_gdp(self):
        return self.rdd.map(lambda x: (x.country_year, (x.suicides_no, x.gdp_per_capital))) \
            .reduceByKey(lambda x, y: (x[0] + y[0], x[1]))
