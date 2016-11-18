package joe.spark.recommender.driver

import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql.SparkSession

import com.typesafe.config.ConfigFactory

import joe.spark.recommender._
import joe.spark.recommender.processor.DataProcessor
import joe.spark.recommender.processor.RecommendationEngine

/**
 *
 * spark-submit --master local[*] --class joe.spark.recommender.driver.RecommenderDriver --conf spark.sql.warehouse.dir=<path-to-warehouse-dir> <path-to-jar>\spark-recommender-0.0.1-SNAPSHOT.jar <file_location> <userId>
 *
 */

object RecommenderDriver {

  @transient lazy val log = org.apache.log4j.LogManager.getLogger(RecommenderDriver.getClass.getName)

  def main(args: Array[String]) {
    //log.info("Starting Recommender")
    println("Starting Recommender")
    val startTime = System.currentTimeMillis()

//    log.info("Using conf file: " + RECOMMENDER_CONF_FILE)
    println("Using conf file: " + RECOMMENDER_CONF_FILE)
    val conf = ConfigFactory.load(RECOMMENDER_CONF_FILE)

    val appName = conf.getString("recommender.appName")
    val numberOfRecommendations = conf.getInt("recommender.numRecommendations")
    val artistFileName = conf.getString("recommender.artist.filename")    
    val userArtistFileName = conf.getString("recommender.userartist.filename")    
    val artistAliasFileName = conf.getString("recommender.artistalias.filename")    

    val fileLocation = args(0).toString
    val userId = args(1).toLong
    
//    log.info("userId: " + userId)
//    log.info("fileLocation: " + fileLocation)
//    log.info("numberOfRecommendations: " + numberOfRecommendations)

    println("userId: " + userId)
    println("fileLocation: " + fileLocation)
    println("numberOfRecommendations: " + numberOfRecommendations)
    
    // Create SparkSession
    val sparkSession = SparkSession.builder.
      appName(appName).
      getOrCreate()


//    if (log.isInfoEnabled()) {
//      log.info("SparkSession Conf:")
//      sparkSession.conf.getAll.foreach( attr => log.info(attr._1 + ": " + attr._2))
//    }  
    
    // Load artist data
    val artistRdd = DataProcessor.readArtistData(sparkSession, artistFileName, fileLocation).rdd

    // Load artist alias data and get a Map
    val artistAliasMap = DataProcessor.readArtistAliasData(sparkSession, artistAliasFileName, fileLocation).collectAsMap()

    // Create a broadcast variable with the alias map
    val artistAliasBc = sparkSession.sparkContext.broadcast(artistAliasMap)

    // Load user/artist data  
    val userArtistRdd = DataProcessor.readUserArtistData(sparkSession, userArtistFileName, fileLocation).rdd

    // 1000246
    // 1000177 (small)
    // 1000142 (small)

    val listeningHistoryRdd = RecommendationEngine.getListeningHistory(userId, userArtistRdd, artistRdd)

//    log.info("========================================")
//    log.info("Listening History:")
//    listeningHistoryRdd.collect().foreach(x => log.info(x.name + " (" + x.artistId + ")"))
    println("========================================")
    println("Listening History:")
    listeningHistoryRdd.collect().foreach(x => println(x.name + " (" + x.artistId + ")"))

    val model = RecommendationEngine.createModel(userArtistRdd, sparkSession, artistAliasBc)

    val recommendationsRdd = RecommendationEngine.getRecommendations(userId, numberOfRecommendations, model, artistRdd)

//    log.info("========================================")
//    log.info("Recommendations:")
//    recommendationsRdd.collect().foreach(x => log.info(x.name + " (" + x.artistId + ")"))
    println("========================================")
    println("Recommendations:")
    recommendationsRdd.collect().foreach(x => println(x.name + " (" + x.artistId + ")"))

//    log.info("========================================\n")
//    log.info("Total execution time: " + (System.currentTimeMillis() - startTime))
    println("========================================\n")
    println("Total execution time: " + (System.currentTimeMillis() - startTime))

    // Stop the SparkSession  
    sparkSession.stop()

  }

}