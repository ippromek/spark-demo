package joe.spark.recommender.driver

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import joe.spark.recommender._
import joe.spark.recommender.processor.DataProcessor

/**
 * 
 * spark-submit --class joe.spark.recommender.driver.RecommenderDriver <path-to-jar>\spark-recommender-0.0.1-SNAPSHOT.jar
 * 
 */

object RecommenderDriver {

  def main(args: Array[String]) {
    println("Starting Recommender")
    println("Using conf file: " + RECOMMENDER_CONF_FILE)
    val conf = ConfigFactory.load(RECOMMENDER_CONF_FILE)

    val appName = conf.getString("recommender.appName")
    val sparkMaster = conf.getString("recommender.sparkMaster")
    val sparkWarehouseDir = conf.getString("recommender.sparkWarehouseDir")
    val sparkLocalDir = conf.getString("recommender.sparkLocalDir")

    // Create SparkSession
    val sparkSession = SparkSession.builder.
      master(sparkMaster).
      appName(appName).
      config("spark.sql.warehouse.dir", sparkWarehouseDir).
      getOrCreate()
    
    // Load user/artist data  
    val userArtistDs = DataProcessor.readUserArtistData(sparkSession, conf)    
    println("Lines in user/artist dataset: " + userArtistDs.count())
    userArtistDs.show()

    // Load artist data
    val artistDs = DataProcessor.readArtistData(sparkSession, conf)    
    println("Lines in artist dataset: " + artistDs.count())
    artistDs.show()
    
    // Load artist alias data
    val artistAliasDs = DataProcessor.readArtistAliasData(sparkSession, conf)    
    println("Lines in artist alias dataset: " + artistAliasDs.count())
    artistAliasDs.show()
    
    // Stop the SparkSession  
    sparkSession.stop()
    
  }

}