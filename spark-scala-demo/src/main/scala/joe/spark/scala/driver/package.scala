package joe.spark.scala

package object driver {
  
  final val STOCK_DRIVER_CONF_FILE = "stock_driver.cof"
  final val REF_DATA_DRIVER_CONF_FILE = "ref_data_driver.conf"
  
  final case class StockRecord(exchange: String, 
                      symbol: String, 
                      date: String, 
                      openPrice: Double, 
                      highPrice: Double, 
                      lowPrice: Double, 
                      closePrice: Double, 
                      volume: Int, 
                      adjClosePrice: Double)
}