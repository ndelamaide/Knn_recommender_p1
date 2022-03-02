package shared

package object predictions
{
  case class Rating(user: Int, item: Int, rating: Double)

  def timingInMs(f : ()=>Double ) : (Double, Double) = {
    val start = System.nanoTime() 
    val output = f()
    val end = System.nanoTime()
    return (output, (end-start)/1000000.0)
  }

  def mean(s :Seq[Double]): Double =  if (s.size > 0) s.reduce(_+_) / s.length else 0.0
  def std(s :Seq[Double]): Double = {
    if (s.size == 0) 0.0
    else {
      val m = mean(s)
      scala.math.sqrt(s.map(x => scala.math.pow(m-x, 2)).sum / s.length.toDouble)
    }
  }

  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def load(spark : org.apache.spark.sql.SparkSession,  path : String, sep : String) : org.apache.spark.rdd.RDD[Rating] = {
       val file = spark.sparkContext.textFile(path)
       return file
         .map(l => {
           val cols = l.split(sep).map(_.trim)
           toInt(cols(0)) match {
             case Some(_) => Some(Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble))
             case None => None
           }
       })
         .filter({ case Some(_) => true 
                   case None => false })
         .map({ case Some(x) => x 
                case None => Rating(-1, -1, -1)})
  }

  def globalavg(ratings : Array[Rating]) : Double = {
    return mean(ratings.map(_.rating).toSeq)
  }

  def user1avg(ratings : Array[Rating]) : Double = {
    
    return mean(ratings.filter(x => x.user == 1).map(_.rating))
  }

  def item1avg(ratings : Array[Rating]) : Double = {
    return mean(ratings.filter(x => x.item == 1).map(_.rating))
  }

  def item1avgdev(ratings : Array[Rating]) : Double = {
    
    return std(ratings.filter(x => x.item == 1).map(_.rating))
  }



  // def scale(rating : org.apache.spark.rdd.RDD[Rating]) : org.apache.spark.rdd.RDD[Rating] = ???

  // def average_rating_per_user(ratings : Array[shared.predictions.Rating]) : Array[shared.predictions.Rating] = ???
    
  //   {
  //   ratings.groupBy(x => x.user).map(x => mean(x))
  // }
}
