# Game Recommendation
This is a Spark Sql based code in Scala to recommend top 6 games per user based on some pretty complicated relationship (;))

## How to run
The package include fat jar plugin to straightaway include with spark-submit command.
The fat jar is built using 
```$xslt
sbt assembly
```
```$xslt
$SPARK_HOME/bin/spark-submit --class game_recommendation.GameRecommendation --master spark://nikya-pavilion:7077 target/scala-2.11/game_recommendation-assembly-1.0.jar
```
It can also run in IDE (tested on IntelliJ)
Or using sbt's usual suspects on command line

```$xslt
sbt
compile
run
```
