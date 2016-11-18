package joe.spark.recommender.processor

import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import com.typesafe.config.Config

import joe.spark.recommender._

object DataProcessor {

  def readUserArtistData(sparkSession: SparkSession, filename: String, fileLocation:String): Dataset[UserArtistRecord] = {
    import sparkSession.implicits._

    // Read in data
    val rawDataDs = sparkSession.read.textFile(fileLocation + filename)
    //    println("Lines read in: " + rawDataDs.count())
    val userArtistDs = rawDataDs.map(_.split(' ')).map(attr => new UserArtistRecord(attr(0).trim().toLong, attr(1).trim().toLong, attr(2).trim().toLong))
    userArtistDs
  }

  def readArtistData(sparkSession: SparkSession, filename: String, fileLocation:String): Dataset[ArtistRecord] = {
    import sparkSession.implicits._

    // Read in data
    val rawDataDs = sparkSession.read.textFile(fileLocation + filename)
    //    println("Lines read in: " + rawDataDs.count())
    val artistDs = rawDataDs.flatMap({ line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toLong, name.trim))
        } catch {
          case e: NumberFormatException => None
        }
      }
    }).map(attr => new ArtistRecord(attr._1, attr._2.trim))
    
    artistDs
  }

  def readArtistAliasData(sparkSession: SparkSession, filename: String, fileLocation:String): PairRDDFunctions[Long, Long] = {
    import sparkSession.implicits._

    val rawDataDs = sparkSession.read.textFile(fileLocation + filename)
    //println("Lines read in from artist alias file: " + rawDataDs.count())
    val artistAliasPRdd = new PairRDDFunctions(rawDataDs.flatMap({ line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        try {
          Some((tokens(0).trim.toLong, tokens(1).trim.toLong))
        } catch {
          case e: NumberFormatException => None
        }
      }
    }).rdd)
    
    artistAliasPRdd
  }

}