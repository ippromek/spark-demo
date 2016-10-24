package joe.spark

package object recommender {

  final val RECOMMENDER_CONF_FILE = "recommender.conf"
  
  final case class UserArtistRecord(userId: Long, 
                      artistId: Long, 
                      playCount: Long)                      

  final case class ArtistRecord(artistId: Long, 
                      name: String)                      
  
  
}