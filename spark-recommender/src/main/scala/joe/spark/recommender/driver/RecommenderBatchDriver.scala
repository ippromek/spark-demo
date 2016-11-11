package joe.spark.recommender.driver

import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql.SparkSession

import com.typesafe.config.ConfigFactory

import joe.spark.recommender._
import joe.spark.recommender.processor.DataProcessor
import joe.spark.recommender.processor.RecommendationEngine

/**
 *
 * spark-submit --master local[*] --class joe.spark.recommender.driver.RecommenderBatchDriver --conf spark.sql.warehouse.dir=<path-to-warehouse-dir> <path-to-jar>\spark-recommender-0.0.1-SNAPSHOT.jar
 *
 */

object RecommenderBatchDriver {

  @transient lazy val log = org.apache.log4j.LogManager.getLogger(RecommenderBatchDriver.getClass.getName)

  def main(args: Array[String]) {
    log.info("Starting RecommenderBatchDriver")
    log.info("Using conf file: " + RECOMMENDER_BATCH_CONF_FILE)
    val conf = ConfigFactory.load(RECOMMENDER_BATCH_CONF_FILE)

    val appName = conf.getString("recommender.batch.appName")
    val numberOfRecommendations = conf.getInt("recommender.batch.numRecommendations")    

    log.info("numberOfRecommendations: " + numberOfRecommendations)
    
    // Create SparkSession
    val sparkSession = SparkSession.builder.
      appName(appName).
      getOrCreate()

    if (log.isInfoEnabled()) {
      log.info("SparkSession Conf:")
      sparkSession.conf.getAll.foreach( attr => log.info(attr._1 + ": " + attr._2))
    }  
    
    // Load artist data
    val artistRdd = DataProcessor.readArtistData(sparkSession, conf).rdd

    // Load artist alias data and get a Map
    val artistAliasMap = DataProcessor.readArtistAliasData(sparkSession, conf).collectAsMap()

    // Create a broadcast variable with the alias map
    val artistAliasBc = sparkSession.sparkContext.broadcast(artistAliasMap)

    // Load user/artist data  
    val userArtistRdd = DataProcessor.readUserArtistData(sparkSession, conf).rdd

    // 1000246
    // 1000177 (small)
    // 1000142 (small)
    
//    val listeningHistoryRdd = RecommendationEngine.getListeningHistory(userId, userArtistRdd, artistRdd)

    log.info("========================================")
    log.info("Listening History:")
//    listeningHistoryRdd.collect().foreach(x => log.info(x.name + " (" + x.artistId + ")"))

    val model = RecommendationEngine.createModel(userArtistRdd, sparkSession, artistAliasBc)

//    val recommendationsRdd = RecommendationEngine.getRecommendations(userId, numberOfRecommendations, model, userArtistRdd, artistRdd)

    log.info("========================================")
    log.info("Recommendations:")
//    recommendationsRdd.collect().foreach(x => log.info(x.name + " (" + x.artistId + ")"))

    log.info("========================================")

    // Stop the SparkSession  
    sparkSession.stop()

  }

}