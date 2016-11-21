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
    val artistFileName = conf.getString("recommender.batch.artist.filename")    
    val userArtistFileName = conf.getString("recommender.batch.userartist.filename")    
    val artistAliasFileName = conf.getString("recommender.batch.artistalias.filename")    
    
    val fileLocation = args(0).toString

    log.info("numberOfRecommendations: " + numberOfRecommendations)
    
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
    artistRdd.persist()

    // Load artist alias data and get a Map
    val artistAliasMap = DataProcessor.readArtistAliasData(sparkSession, artistAliasFileName, fileLocation).collectAsMap()

    // Create a broadcast variable with the alias map
    val artistAliasBc = sparkSession.sparkContext.broadcast(artistAliasMap)

    // Load user/artist data  
    val userArtistRdd = DataProcessor.readUserArtistData(sparkSession, userArtistFileName, fileLocation).rdd
    userArtistRdd.persist()

    log.info("Number of user artist records: " + userArtistRdd.count())

    val userIdRDD = userArtistRdd.map( x => x.userId ).distinct()
    log.info("Number of unique users: " + userIdRDD.count())
    
    val model = RecommendationEngine.createModel(userArtistRdd, sparkSession, artistAliasBc)

    // TODO: Iterate over the set of unique users and get recommendations for each user.

    
    // Stop the SparkSession  
    sparkSession.stop()

  }

}