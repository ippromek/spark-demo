package joe.spark.scala.driver

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.util.Utils
import org.apache.spark.util.Utils
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config

/**
 * 
 * spark-submit --class joe.spark.scala.driver.RefDataDriver <path-to-jar>\spark-scala-demo-0.0.1-SNAPSHOT.jar
 *
 */

object RefDataDriver {

  def main(args: Array[String]) {

    println("Starting RefDataDriver")
    println("Using conf file: " + REF_DATA_DRIVER_CONF_FILE)
    val conf = ConfigFactory.load(REF_DATA_DRIVER_CONF_FILE)
    
    val appName = conf.getString("refdatadriver.appName")
    val sparkMaster = conf.getString("refdatadriver.sparkMaster")
    val sparkWarehouseDir = conf.getString("refdatadriver.sparkWarehouseDir")
    val sparkLocalDir = conf.getString("refdatadriver.sparkLocalDir")

    val sparkSession = SparkSession.builder.
      master(sparkMaster).
      appName(appName).
      config("spark.sql.warehouse.dir", sparkWarehouseDir).
      //        config("spark.local.dir", sparkLocalDir).
      getOrCreate()

    //    sparkSession.conf.getAll.foreach { x => println(x) }

    val countryDf = readCountryRefData(sparkSession, conf) 
    println("COUNTRY count = " + countryDf.count())
    countryDf.show()
//    countryDf.foreach { x => println(x.getString(x.fieldIndex("NAME"))) }

    val stateDf = readStateRefData(sparkSession, conf) 
    println("STATE count = " + stateDf.count())
    stateDf.show()
//    stateDf.foreach { x => println(x.getString(x.fieldIndex("NAME"))) }
    
    sparkSession.stop()

  }

  def readStateRefData(sparkSession: SparkSession, config: Config): DataFrame = {
    readRefData(sparkSession, config, "(SELECT * from STATE)")
  }

  def readCountryRefData(sparkSession: SparkSession, config: Config): DataFrame = {
    readRefData(sparkSession, config, "(SELECT * from COUNTRY)")
  }
  
  def readRefData(sparkSession: SparkSession, config: Config, dbTable: String): DataFrame = {

    val jdbcDf = sparkSession.read
      .format("jdbc")
      .option("driver", config.getString("refdatadriver.db.driver"))
      .option("url", config.getString("refdatadriver.db.url"))
      .option("user", config.getString("refdatadriver.db.user"))
      .option("password", config.getString("refdatadriver.db.password"))
      .option("dbtable", dbTable)
      .load()

    jdbcDf
  }

}