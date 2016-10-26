package joe.spark.recommender.driver

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql.SparkSession

import com.typesafe.config.ConfigFactory

import joe.spark.recommender._
import joe.spark.recommender.processor.DataProcessor
import joe.spark.recommender.processor.RecommendationEngine

/**
 *
 * spark-submit --class joe.spark.recommender.driver.RecommenderDriver <path-to-jar>\spark-recommender-0.0.1-SNAPSHOT.jar
 *
 */

object RecommenderDriver {

  @transient lazy val log = org.apache.log4j.LogManager.getLogger(RecommenderDriver.getClass.getName)

  def main(args: Array[String]) {
    log.info("Starting Recommender")
    log.info("Using conf file: " + RECOMMENDER_CONF_FILE)
    val conf = ConfigFactory.load(RECOMMENDER_CONF_FILE)

    val appName = conf.getString("recommender.appName")
    val sparkMaster = conf.getString("recommender.sparkMaster")
    val sparkWarehouseDir = conf.getString("recommender.sparkWarehouseDir")
    val sparkLocalDir = conf.getString("recommender.sparkLocalDir")
    val numberOfRecommendations = conf.getInt("recommender.numRecommendations")
    val userId = args(0).toLong
    
    log.info("userId: " + userId)
    log.info("numberOfRecommendations: " + numberOfRecommendations)
    
    // Create SparkSession
    val sparkSession = SparkSession.builder.
      master(sparkMaster).
      appName(appName).
      config("spark.sql.warehouse.dir", sparkWarehouseDir).
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

    val listeningHistoryRdd = RecommendationEngine.getListeningHistory(userId, userArtistRdd, artistRdd)

    log.info("========================================")
    log.info("Listening History:")
    listeningHistoryRdd.collect().foreach(x => log.info(x.name + " (" + x.artistId + ")"))

    val model = RecommendationEngine.createModel(userArtistRdd, sparkSession, artistAliasBc)

    val recommendationsRdd = RecommendationEngine.getRecommendations(userId, numberOfRecommendations, model, userArtistRdd, artistRdd)

    log.info("========================================")
    log.info("Recommendations:")
    recommendationsRdd.collect().foreach(x => log.info(x.name + " (" + x.artistId + ")"))

    log.info("========================================")

    // Stop the SparkSession  
    sparkSession.stop()

  }

}