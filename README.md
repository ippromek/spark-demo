# spark-demo
[Travis CI](https://travis-ci.org) Build Status: [![Build Status](https://travis-ci.org/jsicree/spark-demo.svg)](https://travis-ci.org/jsicree/spark-demo)
## Sample command lines
### spark-scala-demo

`spark-submit --class joe.spark.scala.driver.StockDriver C:\Users\jsicree\git\repos\spark-demo\spark-scala-demo\target\spark-scala-demo-0.0.1-SNAPSHOT.jar`

`spark-submit --class joe.spark.scala.driver.RefDataDriver C:\Users\jsicree\git\repos\spark-demo\spark-scala-demo\target\spark-scala-demo-0.0.1-SNAPSHOT.jar`

### spark-recommender

`spark-submit --class joe.spark.recommender.driver.RecommenderDriver C:\Users\jsicree\git\repos\spark-demo\spark-recommender\target\spark-recommender-0.0.1-SNAPSHOT.jar <userId>`

## Sample Output
```
c:\Users\jsicree\dev>spark-submit --class joe.spark.recommender.driver.RecommenderDriver C:\Users\jsicree\git\repos\spark-demo\spark-recommender\target\spark-recommender-0.0.1-SNAPSHOT.jar 1000177
16/10/25 23:08:54 INFO RecommenderDriver$: Starting Recommender
16/10/25 23:08:54 INFO RecommenderDriver$: Using conf file: recommender.conf
16/10/25 23:08:54 INFO RecommenderDriver$: userId: 1000177
16/10/25 23:08:54 INFO RecommenderDriver$: numberOfRecommendations: 5
16/10/25 23:08:56 INFO RecommenderDriver$: SparkSession Conf:
16/10/25 23:08:56 INFO RecommenderDriver$: spark.sql.warehouse.dir: file:///C:/tmp/spark-warehouse
16/10/25 23:08:56 INFO RecommenderDriver$: spark.driver.host: 169.254.156.121
16/10/25 23:08:56 INFO RecommenderDriver$: spark.eventLog.enabled: true
16/10/25 23:08:56 INFO RecommenderDriver$: spark.driver.port: 56720
16/10/25 23:08:56 INFO RecommenderDriver$: hive.metastore.warehouse.dir: file:///C:/tmp/spark-warehouse
16/10/25 23:08:56 INFO RecommenderDriver$: spark.jars: file:/C:/Users/jsicree/git/repos/spark-demo/spark-recommender/target/spark-recommender-0.0.1-SNAPSHOT.jar
16/10/25 23:08:56 INFO RecommenderDriver$: spark.app.name: Recommender
16/10/25 23:08:56 INFO RecommenderDriver$: spark.driver.memory: 2g
16/10/25 23:08:56 INFO RecommenderDriver$: spark.executor.id: driver
16/10/25 23:08:56 INFO RecommenderDriver$: spark.submit.deployMode: client
16/10/25 23:08:56 INFO RecommenderDriver$: spark.master: local[*]
16/10/25 23:08:56 INFO RecommenderDriver$: spark.executor.memory: 2g
16/10/25 23:08:56 INFO RecommenderDriver$: spark.app.id: local-1477451336006
16/10/25 23:09:06 INFO RecommendationEngine$: In getListeningHistory
16/10/25 23:09:06 INFO RecommendationEngine$: Existing Products for 1000177: 5
16/10/25 23:09:06 INFO RecommendationEngine$: Leaving getListeningHistory
16/10/25 23:09:06 INFO RecommenderDriver$: ========================================
16/10/25 23:09:06 INFO RecommenderDriver$: Listening History:
16/10/25 23:09:07 INFO RecommenderDriver$: Kraftwerk (40)
16/10/25 23:09:07 INFO RecommenderDriver$: Survivor (1002614)
16/10/25 23:09:07 INFO RecommenderDriver$: The Beatles (1000113)
16/10/25 23:09:07 INFO RecommenderDriver$: Coldplay (1177)
16/10/25 23:09:07 INFO RecommenderDriver$: R÷yksopp (1001864)
16/10/25 23:09:07 INFO RecommendationEngine$: In createModel
16/10/25 23:09:14 INFO RecommendationEngine$: Leaving createModel
16/10/25 23:09:14 INFO RecommendationEngine$: In getRecommendations
16/10/25 23:09:15 INFO RecommendationEngine$: Leaving getRecommendations
16/10/25 23:09:15 INFO RecommenderDriver$: ========================================
16/10/25 23:09:15 INFO RecommenderDriver$: Recommendations:
16/10/25 23:09:16 INFO RecommenderDriver$: RJD2 (5270)
16/10/25 23:09:16 INFO RecommenderDriver$: Massive Attack (59)
16/10/25 23:09:16 INFO RecommenderDriver$: Radiohead (979)
16/10/25 23:09:16 INFO RecommenderDriver$: DJ Shadow (118)
16/10/25 23:09:16 INFO RecommenderDriver$: R÷yksopp (1001864)
16/10/25 23:09:16 INFO RecommenderDriver$: ========================================
```
