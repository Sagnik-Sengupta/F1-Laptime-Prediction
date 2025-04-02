# F1 Laptime Prediction 🏎️🏁

## Overview
This project predicts **Q3 lap times** of Formula 1 races using **historical qualifying data**. The model is trained using Q1 and Q2 times from past races and then predicts Q3 times for the last race of each season.

## Data Collection
The dataset consists of F1 qualifying results from **2018 to 2024**, stored in CSV format. The data includes:
- **DriverNumber**: Unique identifier for each driver
- **Driver**: Driver's full name
- **TeamName**: Constructor team name
- **FP1, FP2, FP3**: Practice session lap times (not used in final model)
- **Q1, Q2, Q3**: Qualifying session lap times
- **Year**: Season year
- **Round**: Race number in the season

## Methodology
1. **Data Loading:** The data is stored in HDFS and read using **Apache Spark**.
2. **Data Preprocessing:**
   - Convert lap times to numeric format.
   - Remove rows where necessary qualifying times (Q1, Q2, or Q3) are missing.
3. **Model Training:**
   - A **Linear Regression model** is trained per season.
   - Training data includes all races except the last round.
   - Testing data includes only the last round with known Q1 and Q2 times.
4. **Predictions:**
   - The model predicts Q3 times for the final race.
   - Results are stored in HDFS under `/f1_predictions_only_quali/`.
   - Predictions are sorted by the predicted Q3 times.

## Directory Structure
```
/f1_laptime_prediction/
│── datasets/                   # Raw F1 datasets (2018-2024)
│── src/                    # Python scripts for training and predictions
│── f1_results/                    # Prediction results stored here
│── README.md                   # Project documentation
```

## Running the Project
### Prerequisites
- Apache Spark
- Hadoop (HDFS)
- PySpark
- Git

### Steps to Execute
1. Move datasets to HDFS:
   ```bash
   hdfs dfs -mkdir -p /f1_data
   hdfs dfs -put datasets/*.csv /f1_data/
   ```
2. Run the model training script:
   ```bash
   spark-submit scripts/model_training_hdfs_only_quali.py
   ```
3. Retrieve Predictions:
   ```bash
   hdfs dfs -get /f1_predictions_only_quali/ results/
   ```

## GitHub Repository
To push the project to GitHub:
```bash
git init
git remote add origin https://github.com/YOUR_USERNAME/f1_laptime_prediction.git
git add .
git commit -m "Initial commit"
git push -u origin main
```

## Contributors
- **Sagnik Sengupta**  

## License
MIT License

