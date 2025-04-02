from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, lit, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.appName("F1_Q3_Prediction_HDFS").getOrCreate()

hdfs_base = "hdfs://localhost:9000"
seasons = range(2018, 2025)

for year in seasons:
    data_path = f"{hdfs_base}/f1_data/f1_data_{year}.csv"
    df = spark.read.csv(data_path, header=True, inferSchema=True)
    
    for col_name in ["Q1", "Q2", "Q3"]:
        df = df.withColumn(col_name, col(col_name).cast("float"))
    
    max_round = df.agg(spark_max("Round")).collect()[0][0]
    
    train_data = df.filter((col("Year") == year) & (col("Round") != max_round))
    test_data = df.filter((col("Year") == year) & (col("Round") == max_round))
    
    train_data = train_data.filter(col("Q1").isNotNull() & col("Q2").isNotNull() & col("Q3").isNotNull())
    test_data = test_data.filter(col("Q1").isNotNull() & col("Q2").isNotNull())
    
    test_data = test_data.withColumn("Actual_Q3", when(col("Q3").isNotNull(), col("Q3").cast("string")).otherwise(lit("q3 time invalid")))
    
    if train_data.count() == 0:
        print(f"No valid training data for season {year}. Skipping...")
        continue
    
    print(f"Available columns for {year}: {train_data.columns}")
    
    if "Q3" not in train_data.columns:
        print(f"Q3 column is missing for {year}. Skipping training...")
        continue
    
    assembler = VectorAssembler(inputCols=["Q1", "Q2"], outputCol="features")
    lr = LinearRegression(featuresCol="features", labelCol="label")
    
    train_data = assembler.transform(train_data).select("features", col("Q3").alias("label"))
    model = lr.fit(train_data)
    
    test_data = assembler.transform(test_data).select("Driver", "TeamName", "Q1", "Q2", "Actual_Q3", "features")
    predictions = model.transform(test_data).select("Driver", "TeamName", "Q1", "Q2", "Actual_Q3", col("prediction").alias("Predicted_Q3")).orderBy("Predicted_Q3")
    
    output_path = f"{hdfs_base}/f1_predictions_only_quali/predicted_q3_{year}_last_race.csv"
    predictions.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
    
    print(f"Season {year}: Predictions saved to HDFS at '{output_path}'.")

spark.stop()
