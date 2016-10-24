package joe.spark.recommender.processor

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.ALS
import joe.spark.recommender._
import org.apache.spark.broadcast.Broadcast

object RecommendationEngine {

  def createModel(userArtistRdd: RDD[UserArtistRecord], sparkSession: SparkSession, artistAliasBc: Broadcast[scala.collection.Map[Long, Long]]): MatrixFactorizationModel = {

    import sparkSession.implicits._

    val trainData = userArtistRdd.map { userArtistRecord =>
      val finalArtistId = artistAliasBc.value.getOrElse(userArtistRecord.artistId, userArtistRecord.artistId)
      Rating(userArtistRecord.userId.toInt, finalArtistId.toInt, userArtistRecord.playCount)
    }.cache()

    //    trainData.take(100).foreach { x => println(x.user + ", " + x.product + ", " + x.rating) }
    val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)
    model

  }
}