package joe.spark

package object scala {
  
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