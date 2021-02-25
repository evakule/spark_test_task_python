class SuicideStatisticCalculation:

    def __init__(self, data):
        self.data = data

    def get_top_five(self, data):
        return data.rdd.map(lambda x: (x[7], x[4])) \
            .reduceByKey(lambda x, y: x + y) \
            .map(lambda x: (x[1], x[0])) \
            .sortByKey(ascending=False) \
            .map(lambda x: (x[1], x[0])).take(5)

    def get_avg_suicide_by_one_hundred(self, data):
        return data.rdd.map(lambda x: (x[7], (1, x[6]))) \
            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
            .map(lambda x: (x[0], x[1][1] / x[1][0])) \
            .map(lambda x: (x[1], x[0])) \
            .sortByKey(ascending=False) \
            .map(lambda x: (x[1], x[0]))

    def output_refactor(self, row):
        colon_index = row[0].index(':')
        country = row[0][0:colon_index + 1]
        years_and_amount_of_suicides = row[0][colon_index + 1:] + str(row[1])
        return (country, years_and_amount_of_suicides)

    def get_country_age_distribution(self, data):
        return data.rdd.map(lambda x: (x[0] + ': ' + x[3] + ' = ', x[4])) \
            .reduceByKey(lambda x, y: x + y) \
            .map(lambda x: self.output_refactor(x)) \
            .reduceByKey(lambda x, y: x + ',' + y)

    def get_country_year_to_suicide_amount_to_gdp(self, data):
        return data.rdd.map(lambda x: (x[7], (x[4], x[10]))) \
            .reduceByKey(lambda x, y: (x[0] + y[0], x[1]))
