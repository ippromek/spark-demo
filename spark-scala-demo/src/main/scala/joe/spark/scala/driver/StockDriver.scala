package joe.spark.scala.driver

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import joe.spark.scala.processor.StockProcessor
import joe.spark.scala.driver._
import joe.spark.scala._

/**
 * 
 * spark-submit --class joe.spark.scala.driver.StockDriver <path-to-jar>\spark-scala-demo-0.0.1-SNAPSHOT.jar
 * 
 */

object StockDriver {

  def main(args: Array[String]) { 
    println("Starting StockDriver")
    println("Using conf file: " + STOCK_DRIVER_CONF_FILE)
    val conf = ConfigFactory.load(STOCK_DRIVER_CONF_FILE)
    
    val appName = conf.getString("stockdriver.appName")
    val sparkMaster = conf.getString("stockdriver.sparkMaster")
    val sparkWarehouseDir = conf.getString("stockdriver.sparkWarehouseDir")
    val sparkLocalDir = conf.getString("stockdriver.sparkLocalDir")
    val inputFileName = conf.getString("stockdriver.input.fileName")
        
    val sparkSession = SparkSession.builder.
      master(sparkMaster).
      appName(appName).
      config("spark.sql.warehouse.dir", sparkWarehouseDir).
      getOrCreate()

    val stockRecordDs = StockProcessor.loadStockRecords(sparkSession, inputFileName)
    stockRecordDs.show(10)
    println("Number of lines read: " + stockRecordDs.collect().size)
    
    val uniqueDf = stockRecordDs.groupBy("stockSymbol")
    uniqueDf.count().show()
    
    sparkSession.stop()

  }

}