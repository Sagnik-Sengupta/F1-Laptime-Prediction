from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, lit, when
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("F1_Q3_Prediction_HDFS").getOrCreate()

hdfs_base = "hdfs://localhost:9000"  

seasons = range(2018, 2025)

for year in seasons:
    data_path = f"{hdfs_base}/f1_data/f1_data_{year}.csv"
    df = spark.read.csv(data_path, header=True, inferSchema=True)
    numeric_columns = ["FP1", "FP2", "FP3", "Q1", "Q2", "Q3"]
    
    for col_name in numeric_columns:
        df = df.withColumn(col_name, col(col_name).cast("float"))
        
    max_round = df.agg(spark_max("Round")).collect()[0][0]
    train_required = ["DriverNumber", "TeamName", "FP1", "FP2", "FP3", "Q1", "Q2", "Q3"]
    train_data = df.filter((col("Year") == year) & (col("Round") != max_round))
    
    for field in train_required:
        train_data = train_data.filter(col(field).isNotNull())
        
    test_required = ["DriverNumber", "TeamName", "FP1", "FP2", "FP3", "Q1", "Q2"]
    test_data = df.filter((col("Year") == year) & (col("Round") == max_round))
    
    for field in test_required:
        test_data = test_data.filter(col(field).isNotNull())
        
    test_data = test_data.withColumn("Actual_Q3", when(col("Q3").isNotNull(), col("Q3").cast("string")).otherwise(lit("q3 time invalid")))
    team_indexer = StringIndexer(inputCol="TeamName", outputCol="TeamIndex", handleInvalid="keep")
    assembler = VectorAssembler(inputCols=["DriverNumber", "TeamIndex", "FP1", "FP2", "FP3", "Q1", "Q2"], outputCol="features")
    lr = LinearRegression(featuresCol="features", labelCol="Q3")
    pipeline = Pipeline(stages=[team_indexer, assembler, lr])
    
    if train_data.count() == 0:
        print(f"No valid training data for season {year}. Skipping...")
        continue
        
    model_pipeline = pipeline.fit(train_data)
    test_transformed = model_pipeline.transform(test_data)
    predictions = test_transformed.select(
        "Driver",
        "TeamName",
        "DriverNumber",
        "FP1",
        "FP2",
        "FP3",
        "Q1",
        "Q2",
        "Actual_Q3",
        col("prediction").alias("Predicted_Q3")
    ).orderBy("Predicted_Q3")
    
    output_path = f"{hdfs_base}/f1_predictions/predicted_q3_{year}_last_race.csv"
    predictions.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
    print(f"Season {year}: Predictions saved to HDFS at '{output_path}'.")

spark.stop()
