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
    
    sparkSession.sparkContext.getConf.getAll.foreach( attr => println(attr._1 + ": " + attr._2))
    
      
    // Load artist data
    val artistRdd = DataProcessor.readArtistData(sparkSession, conf).rdd    
//    println("Lines in artist dataset: " + artistDs.count())    
//    artistDs.show()
    
    // Load artist alias data and get a Map
    val artistAliasMap = DataProcessor.readArtistAliasData(sparkSession, conf).collectAsMap()    

    // Create a broadcast variable with the alias map
    val artistAliasBc = sparkSession.sparkContext.broadcast(artistAliasMap)
    
    // Load user/artist data  
    val userArtistRdd = DataProcessor.readUserArtistData(sparkSession, conf).rdd    
//    println("Lines in user/artist dataset: " + userArtistDs.count())
//    userArtistDs.show()
    
//    import sparkSession.implicits._
//
//    val trainData = userArtistRdd.map { userArtistRecord =>
//      val finalArtistId = artistAliasBc.value.getOrElse(userArtistRecord.artistId, userArtistRecord.artistId)
//      Rating(userArtistRecord.userId.toInt, finalArtistId.toInt, userArtistRecord.playCount)      
//    }.cache()
//
////    trainData.take(100).foreach { x => println(x.user + ", " + x.product + ", " + x.rating) }
//
//    val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)

    val model = RecommendationEngine.createModel(userArtistRdd, sparkSession, artistAliasBc)
    
    
    
//    val output = model.userFeatures.mapValues(_.mkString(", ")).first
//    println("Output: " + output._1 + "; " + output._2)

    // 1000246
    // 1000177 (small)
    // 1000142 (small)
    val artistsForUser = userArtistRdd.filter { record => record.userId == 1000142 }
    
    val existingProducts = artistsForUser.map { record => record.artistId }.collect().toSet
    println("Existing Products: " + existingProducts.size)
    
    println("\nListening History:")
    artistRdd.filter { record => existingProducts.contains(record.artistId) }.collect().foreach(x => println(x.name))
    

    val recommendations = model.recommendProducts(1000142, 5)
    
    val recommendedProductIds = recommendations.map(_.product).toSet

    println("\nRecommendations:")
    artistRdd.filter { record => recommendedProductIds.contains(record.artistId.toInt)}.collect().foreach(x => println(x.name))

    println("========================================")
    println("\n\n\n\n\n\n\n")
    
    
    // Stop the SparkSession  
    sparkSession.stop()
    
  }

}