package shared

import org.apache.spark.rdd.RDD

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

  def scale(x: Double, useravg: Double): Double = {
    if (x > useravg)
      5 - useravg
    else if (x < useravg)
      useravg - 1
    else
      1
  }

  // Spark implementations below
  // TODO : NaN values for ml-100k dataset -> Empty table ?
  // -> Works fine for 25M dataset
  def globalavg_spark(ratings: RDD[Rating]) : Double = {
    if (ratings.isEmpty()) {
      println("Ratings is empty")
      return 0
    }
    else ratings.map(x => x.rating).sum() / ratings.count()
  }

  def useravg_spark(ratings: RDD[Rating], usr: Double) : Double = {
    return globalavg_spark(ratings.filter(x => (x.user == usr)))
  }

  def itemavg_spark(ratings: RDD[Rating], itm: Double) : Double = {
    return globalavg_spark(ratings.filter(x => (x.item == itm)))
  }

  def normalizeddev_spark(ratings: RDD[Rating], usr: Double, itm: Double) : Double = {
    val rui = ratings.filter(x => ((x.user == usr) && (x.item == itm))).first().rating
    val ru = useravg_spark(ratings, usr)

    return (rui - ru) / scale(rui, ru)
  }

  def itemavgdev_spark(ratings: RDD[Rating], itm: Double) : Double = {
      val user_set = ratings.filter(x => (x.item == itm))

      if (user_set.isEmpty()) 0
      // below doesn't work unless use collect then toSeq because of the call to normalizedev inside map
      // use (key, value) pairs and aggregate after maybe because otherwise very slow on cluster
      else user_set.collect().toSeq.map(x => normalizeddev_spark(ratings, x.user, x.item)).sum / user_set.count() 
  }

  def userpred_spark(ratings: RDD[Rating], usr: Double, itm: Double) : Double = {
    if (ratings.filter(x => (x.user == usr)).isEmpty()) globalavg_spark(ratings)
    else {
      val ru = useravg_spark(ratings, usr)
      val ri = itemavg_spark(ratings, itm)
      
      return ru + ri * scale(ru + ri, ru)
    }
  }

  // def average_rating_per_user(ratings : Array[shared.predictions.Rating]) : Array[shared.predictions.Rating] = ???
    
  //   {
  //   ratings.groupBy(x => x.user).map(x => mean(x))
  // }
}
