package joe.spark.scala

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
package object driver {
  
  final val STOCK_DRIVER_CONF_FILE = "stock_driver.conf"
  final val REF_DATA_DRIVER_CONF_FILE = "ref_data_driver.conf"
  
  final case class StockRecord(exchange: String, 
                      stockSymbol: String, 
                      date: String, 
                      stockPriceOpen: Double, 
                      stockPriceHigh: Double, 
                      stockPriceLow: Double, 
                      stockPriceClose: Double, 
                      stockVolume: Int, 
                      stockPriceAdjClose: Double)
                      
  final case class State(id: Long, 
                      name: String, 
                      code: String, 
                      countryId: Long)                      

  final case class Country(id: Long, 
                      name: String, 
                      code: String)                      

}