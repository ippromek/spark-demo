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
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
 * 
 * spark-submit --class joe.spark.scala.driver.StockDriver <path-to-jar>\spark-scala-demo-0.0.1-SNAPSHOT.jar stock_driver.conf
 * 
 */

object StockDriver {

  def main(args: Array[String]) {

    println("Starting StockDriver")
    println("Using conf file: " + args(0))
    val conf = ConfigFactory.load(args(0))
    
    val appName = conf.getString("stockdriver.appName")
    val sparkMaster = conf.getString("stockdriver.sparkMaster")
    val sparkWarehouseDir = conf.getString("stockdriver.sparkWarehouseDir")
    val sparkLocalDir = conf.getString("stockdriver.sparkLocalDir")
    val fileName = conf.getString("stockdriver.fileName")
        
    val sparkSession = SparkSession.builder.
      master(sparkMaster).
      appName(appName).
      config("spark.sql.warehouse.dir", sparkWarehouseDir).
      //        config("spark.local.dir", sparkLocalDir).
      getOrCreate()
      
    //    sparkSession.conf.getAll.foreach { x => println(x) }

    val stockRecordDf = loadStockRecords(sparkSession, fileName)
    stockRecordDf.show(10)
    println("Number of lines read: " + stockRecordDf.collect().size)

    //    val minMaxMeanDf = createMinMaxMeanDf(sparkSession, stockRecordDf)

    //processStockSymbol("AVT", sparkSession, stockRecordDf)

    sparkSession.stop()

  }

  def loadStockRecords(sparkSession: SparkSession, fileName: String): DataFrame = {

    val exchangeField = StructField("exchange", StringType, false)
    val symbolField = StructField("stock_symbol", StringType, false)
    // TODO: Convert date to a Date. For now, treat as String
    //    val dateField = StructField("date",DateType, false)
    val dateField = StructField("date", StringType, false)
    val openField = StructField("stock_price_open", DoubleType, false)
    val highField = StructField("stock_price_high", DoubleType, false)
    val lowField = StructField("stock_price_low", DoubleType, false)
    val closeField = StructField("stock_price_close", DoubleType, false)
    val volField = StructField("stock_volume", IntegerType, false)
    val adjField = StructField("stock_price_adj_close", DoubleType, false)

    val record = StructType(Array(exchangeField, symbolField, dateField, openField, highField, lowField, closeField, volField, adjField))

    val dataframe = sparkSession.read.format("csv").option("header", "true").schema(record).load(fileName)

    //    val formatter: DateTimeFormatter = DateTimeFormat.forPattern("MM/dd/yyyy")
    //    val x = dataframe.map {row =>
    //      StockRecord(row.getString(row.fieldIndex("exchangeField")),
    //        row.getString(row.fieldIndex("symbolField")),
    //        formatter.parseLocalDate(row.getString(row.fieldIndex("dateField"))),
    //        row.getDouble(row.fieldIndex("openField")),
    //        row.getDouble(row.fieldIndex("highField")),
    //        row.getDouble(row.fieldIndex("lowField")),
    //        row.getDouble(row.fieldIndex("closeField")),
    //        row.getInt(row.fieldIndex("volField")),
    //        row.getDouble(row.fieldIndex("adjField")))
    //    }
    //    dataframe.map ( data => formatter.parseLocalDate( data.getString(data.fieldIndex("dateField") 
    //    }

    dataframe
  }

  def createMinMaxMeanDf(sparkSession: SparkSession, recordDf: DataFrame): DataFrame = {

    val groupedDf = recordDf.groupBy("stock_symbol")

    val minDf = groupedDf.min("stock_price_close")
    val meanDf = groupedDf.mean("stock_price_close")
    val maxDf = groupedDf.max("stock_price_close")

    // JOIN the Dataframes by stock symbol to get [symbol, {min, mean, max}]
    
    println("Unique stock symbols: " + meanDf.collect().size)
    meanDf.collect().foreach { x => println(x) }

    minDf
  }

  def processStockSymbol(symbol: String, sparkSession: SparkSession, recordDf: DataFrame): Unit = {

    recordDf.createOrReplaceTempView("stock_table")
    val symbolDf = sparkSession.sql("select * from stock_table where stock_symbol='" + symbol + "'")
    println("Number of lines for symbol: " + symbolDf.collect().size)

    val groupedSymbolDf = symbolDf.groupBy("stock_symbol")

    val meanSymbolDf = groupedSymbolDf.mean("stock_price_close")

    meanSymbolDf.collect().foreach { x => println(x) }

    //    val groupedDf = df.groupBy("stock_symbol", "stock_price_close")
    //    
    //    val outDf = groupedDf.max("stock_price_close")
    //    outDf.collect().take(10).foreach { x => println(x) }

    //    df.write.parquet("NYSE_A.parquet")

  }
}