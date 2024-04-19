#!/usr/bin/python
#!/usr/bin/env python

# Gcloud CLI Command :
# gcloud dataproc jobs submit pyspark --cluster=hw13-pyspark-cluster script.py --region=asia-southeast1

from pyspark import  SparkContext
from operator import add
import pandas as pd

# Functions that is going to be used
def reduce_dicts(dict1, dict2):
    result = {}
    for key in dict1.keys():
        result[key] = dict1[key] + dict2.get(key, 0)
    for key in dict2.keys():
        if key not in dict1:
            result[key] = dict2[key]
    return result

def get_app_name(row):
    return row[-1], int(row[4])


input_folder =  'gs://cs273bucket/Input'
output_folder =  'gs://cs273bucket/Output'

input_file1 = 'reviews.csv'

output_file1 = 'ranking_by_sum'
output_file2 = 'ranking_by_average'

sc = SparkContext(appName = "GooglePlayApps")

reviews = sc.textFile(f"{input_folder}/{input_file1}")

reviews_rdd = sc.parallelize(pd.read_csv(f"{input_folder}/{input_file1}").values.tolist())

# Map App Id to score
reviews = reviews_rdd.map(lambda x: get_app_name(x))

# If score > 3 it is `Positive`, score == 3 it is `Neutral` , score < 3 it is `Negative`
review_dict = reviews.mapValues(lambda x:   {"Positive" : 1 if x > 3 else 0, "Neutral": 1 if x == 3 else 0 , "Negative": 1 if x < 3 else 0  } )

# Get the App score by some
review_score = reviews.reduceByKey(add)

# Get the counts of the Sentiments
sentiments_count = review_dict.reduceByKey(reduce_dicts)

# Rank the app by sum of scores
ranking_sum_of_scores = reviews.reduceByKey(add).sortBy(lambda x: x[1], ascending=False).sortBy(lambda x: x[1], ascending=False)
ranking_by_sum_rdd = ranking_sum_of_scores.join(sentiments_count).sortBy(lambda x: x[1][0], ascending=False)

# Rank the app by average of scores
appname_count = reviews.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)
ranking_by_average_rdd = ranking_sum_of_scores.join(appname_count).mapValues(lambda x: x[0] / x[1]).join(sentiments_count).sortBy(lambda x: x[1][0], ascending=False)

# Save the `Ranking of Appname by Sum of Scores`
ranking_by_sum_rdd.saveAsTextFile(f"{output_folder}/{output_file1}")
# Save the `Ranking of Appname by Average of Scores``
ranking_by_average_rdd.saveAsTextFile(f"{output_folder}/{output_file2}")

sc.stop()