package joe.spark.scala.processor

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import joe.spark.scala._

object StockProcessor {
  
  
  def loadStockRecords(sparkSession: SparkSession, fileName: String): Dataset[StockRecord] = {

    import sparkSession.implicits._      

    val exchangeField = StructField("exchange", DataTypes.StringType, false)
    val symbolField = StructField("stockSymbol", DataTypes.StringType, false)
    // TODO: Convert date to a Date. For now, treat as String
    //    val dateField = StructField("date",DateType, false)
    val dateField = StructField("date", DataTypes.StringType, false)
    val openField = StructField("stockPriceOpen", DataTypes.DoubleType, false)
    val highField = StructField("stockPriceHigh", DataTypes.DoubleType, false)
    val lowField = StructField("stockPriceLow", DataTypes.DoubleType, false)
    val closeField = StructField("stockPriceClose", DataTypes.DoubleType, false)
    val volField = StructField("stockVolume", DataTypes.IntegerType, false)
    val adjField = StructField("stockPriceAdjClose", DataTypes.DoubleType, false)

    val record = StructType(Array(exchangeField, symbolField, dateField, openField, highField, lowField, closeField, volField, adjField))
    val dataframe = sparkSession.read.format("csv").option("header", "true").schema(record).load(fileName)
    
    val dataset = dataframe.as[StockRecord]    
    dataset
    
  }

  def createMinMaxMeanDf(sparkSession: SparkSession, recordDs: Dataset[StockRecord]): DataFrame = {

    val groupedDf = recordDs.groupBy("stockSymbol")

    val minDf = groupedDf.min("stockPriceClose")
    val meanDf = groupedDf.mean("stockPriceClose")
    val maxDf = groupedDf.max("stockPriceClose")

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