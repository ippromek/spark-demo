package joe.spark.recommender.processor

import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import com.typesafe.config.Config

import joe.spark.recommender._

object DataProcessor {

  def readUserArtistData(sparkSession: SparkSession, config: Config): Dataset[UserArtistRecord] = {
    import sparkSession.implicits._

    val filename = config.getString("recommender.userartist.filename")

    // Read in data
    val rawDataDs = sparkSession.read.textFile(filename)
    //    println("Lines read in: " + rawDataDs.count())
    val userArtistDs = rawDataDs.map(_.split(' ')).map(attr => new UserArtistRecord(attr(0).trim().toLong, attr(1).trim().toLong, attr(2).trim().toLong))
    userArtistDs
  }

  def readArtistData(sparkSession: SparkSession, config: Config): Dataset[ArtistRecord] = {
    import sparkSession.implicits._

    val filename = config.getString("recommender.artist.filename")

    // Read in data
    val rawDataDs = sparkSession.read.textFile(filename)
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

  def readArtistAliasData(sparkSession: SparkSession, config: Config): PairRDDFunctions[Long, Long] = {
    import sparkSession.implicits._

    val filename = config.getString("recommender.artistalias.filename")
    val rawDataDs = sparkSession.read.textFile(filename)
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