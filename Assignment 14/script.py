from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType

from pyspark import SparkContext, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import col

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator


input_file = "gs://cs273bucket/Input/irdata-v3.csv.gz"
output_folder = "gs://cs273bucket/Output/hw14"

# create spark session
spark = SparkSession.builder.master("local[*]").appName("SparkMLLRApp").getOrCreate()

# read data from filesystem
contraceptionData = spark.read.csv(input_file, header = True, inferSchema = True).cache()

# clean data
contraceptionDataStringReplaced = contraceptionData.na.replace("", "-1")
contraceptionDataCasted = contraceptionDataStringReplaced.withColumn("Religion", col("Religion").cast("float"))
notNull = contraceptionDataCasted.fillna({ 'Used method in last 12 months': 2 })
df = notNull.fillna(-1)

# split data
train, test = df.randomSplit([0.7, 0.3], seed=30)

# assemble ML pipeline using Logistic Regression
assembler = VectorAssembler(
    inputCols=['Age', 'Residence Type', 'Religion', 'Educ in single years', 'Encouraged FP', 'HH Head', 'Land Owner', 'Earns more', 'DM Contraception', 'Depression/ anxiety', 'Total CEB', 'Number of SC'],
    outputCol="features")
model_lr = LogisticRegression(featuresCol = 'features', labelCol = 'Used method in last 12 months', maxIter=10)
classificationPipeline = Pipeline(stages=[assembler, model_lr])
classificationPipelineModel = classificationPipeline.fit(train)

classified = classificationPipelineModel.transform(train)
prediction = classificationPipelineModel.transform(test)

# show pre-crossval accuracy
evaluator = BinaryClassificationEvaluator(rawPredictionCol="prediction", labelCol = "Used method in last 12 months")
acc_before_cv = evaluator.evaluate(prediction)

# Cross Validation
paramGrid = (ParamGridBuilder()
             .addGrid(model_lr.regParam,        [0.01, 0.25, 0.5, 1.0, 2.0])
             .addGrid(model_lr.elasticNetParam, [0.0, 0.25, 0.5, 0.75, 1.0])
             .addGrid(model_lr.maxIter,         [1, 5, 10, 15, 20])
             .build())
cv = CrossValidator(estimator=classificationPipeline, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)
cvModel = cv.fit(train)

# show post crossval accuracy
predictions_cvModel = cvModel.transform(test)
acc_after_cv = evaluator.evaluate(predictions_cvModel)

# Save using dataframe
rdd = spark.sparkContext.parallelize([(acc_before_cv, acc_after_cv)])
schema = StructType([StructField("acc_before_cv", FloatType(), True), StructField("acc_after_cv", FloatType(), True)])
acc = spark.createDataFrame(rdd, schema=schema)
acc.write.format("csv").option("header", "true").save(output_folder)