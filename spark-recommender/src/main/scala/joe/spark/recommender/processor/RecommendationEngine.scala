package joe.spark.recommender.processor

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.broadcast.Broadcast
import joe.spark.recommender._

object RecommendationEngine {

  @transient lazy val log = org.apache.log4j.LogManager.getLogger(RecommendationEngine.getClass.getName)
  
  def createModel(userArtistRdd: RDD[UserArtistRecord], sparkSession: SparkSession, artistAliasBc: Broadcast[scala.collection.Map[Long, Long]]): MatrixFactorizationModel = {
    log.info("In createModel")

    import sparkSession.implicits._
    
    val trainData = userArtistRdd.map { userArtistRecord =>
      val finalArtistId = artistAliasBc.value.getOrElse(userArtistRecord.artistId, userArtistRecord.artistId)
      Rating(userArtistRecord.userId.toInt, finalArtistId.toInt, userArtistRecord.playCount)
    }.cache()

    //    trainData.take(100).foreach { x => println(x.user + ", " + x.product + ", " + x.rating) }
    val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)
    log.info("Leaving createModel")
    
    model

  }
  
  def getListeningHistory(userId: Long, userArtistRdd: RDD[UserArtistRecord], artistRdd: RDD[ArtistRecord]): RDD[ArtistRecord] = {

    log.info("In getListeningHistory")
    val artistsForUser = userArtistRdd.filter { record => record.userId == userId }
    
    val existingProducts = artistsForUser.map { record => record.artistId }.collect().toSet
    log.info("Existing Products for " + userId + ": " + existingProducts.size)
    
    val listeningHistoryRdd = artistRdd.filter { record => existingProducts.contains(record.artistId) }
    log.info("Leaving getListeningHistory")
    listeningHistoryRdd    
  }
  
  
  def getRecommendations(userId: Long, numRecommendations: Int, model: MatrixFactorizationModel, userArtistRdd: RDD[UserArtistRecord], artistRdd: RDD[ArtistRecord]): RDD[ArtistRecord] = {
  
    log.info("In getRecommendations")
    val recommendations = model.recommendProducts(userId.toInt, numRecommendations)
    
    val recommendedProductIds = recommendations.map(_.product).toSet

    val recommendationRdd = artistRdd.filter { record => recommendedProductIds.contains(record.artistId.toInt)}

    log.info("Leaving getRecommendations")
    recommendationRdd
  } 
  
}