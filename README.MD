# Internship task for JetBrains
## BigData ETL project


Database connection properties are located in the project.properties file.
If you want to use a DB other than PostgresQL, you should manually add dependency for its Driver.

### Setup
```
mvn package
```
### Usage


```
$SPARK_HOME/bin/spark-submit --master local[*] target/bigdataETL-1.0-SNAPSHOT.jar GlobalLandTemperaturesByMajorCity.csv
```