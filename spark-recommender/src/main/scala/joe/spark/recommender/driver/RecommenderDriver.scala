package joe.spark.recommender.driver

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import joe.spark.recommender._

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
    val filename = conf.getString("recommender.input.fileName")

    println("Filename = " + filename)

    // Create SparkSession
    val sparkSession = SparkSession.builder.
      master(sparkMaster).
      appName(appName).
      config("spark.sql.warehouse.dir", sparkWarehouseDir).
      getOrCreate()
      
    // Read in data
    val rawDataDs = sparkSession.read.textFile(filename)
    println("Lines read in: " + rawDataDs.count())
    
    // Stop the SparkSession  
    sparkSession.stop()
    
  }

}