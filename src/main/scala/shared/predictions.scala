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

  def globalavg_spark(ratings: RDD[Rating]): Double = {
    return ratings.map(x => x.rating).mean()
  }

  def user1avg_spark(ratings: RDD[Rating]): Double = {
    return globalavg_spark(ratings.filter(x => x.user == 1))
  }

  def item1avg_spark(ratings: RDD[Rating]): Double = {
    return globalavg_spark(ratings.filter(x => x.item == 1))
  }

  def item1avgdev_spark(ratings: RDD[Rating]): Double = {
    val item1rating_set = ratings.filter(x => (x.item == 1))
    val usersavg = item1rating_set.map(x => (x.user, (x.rating, 1))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2 )).mapValues(v => v._1 / v._2).collect().toMap
                    //.map(x => x.user).collect().map(x => (x, globalavg_spark(ratings.filter(y => y.user == x)))).toMap
                    // not same result with commented option
    
    item1rating_set.map(x => (x.rating - usersavg(x.user)) / scale(x.rating, usersavg(x.user))).sum() / item1rating_set.count()
  }

  def user1pred_spark(ratings: RDD[Rating]): Double = {
    if (ratings.filter(x => x.user == 1).isEmpty()) user1avg_spark(ratings)
    else {
      val ru = user1avg_spark(ratings)
      val ri = item1avgdev_spark(ratings)
      return ru + ri * scale(ri + ru, ru)
    }
  }

  def standardize_spark(ratings: RDD[Rating]): RDD[Rating] = {

    val usersavg = ratings.map(x => (x.user, (x.rating, 1))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2 )).mapValues(v => v._1 / v._2).collect().toMap
    
    return ratings.map(x => Rating(x.user, x.item, (x.rating - usersavg(x.user)) / scale(x.rating, usersavg(x.user))))
  }

  def pred_spark(ratings: RDD[Rating], ratings_standardized: RDD[Rating], usr: Int, itm: Int): Double = {
    val user_set = ratings_standardized.filter(x => x.item == itm)
    val usr_ratings = ratings.filter(x => x.user == usr)
    val ru = globalavg_spark(usr_ratings)
    val ri = user_set.map(x => x.rating).mean()

    if (usr_ratings.isEmpty()) globalavg_spark(ratings)
    else ru + ri * scale((ru + ri), ru)
  }

  /*
  def globalavg_spark(ratings: RDD[Rating]) : Double = {
    if (ratings.isEmpty()) 0
    else ratings.map(x => x.rating).sum() / ratings.count()
  }

  def useravg_spark(ratings: RDD[Rating], usr: Double) : Double = {
    return globalavg_spark(ratings.filter(x => (x.user == usr)))
  }

  def itemavg_spark(ratings: RDD[Rating], itm: Double) : Double = {
    return globalavg_spark(ratings.filter(x => (x.item == itm)))
  }

  def globalavgdev_spark(ratings: RDD[Rating], itm: Double) : Double = {
      
      val user_set = ratings.filter(x => (x.item == itm))

      println("Size user set ", user_set.count())

      // Compute ru_bar -> avg rating for each user that has rated item itm
      val useravgs = user_set.map(x => (x.user, (x.rating, 1))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2 ))
                     .mapValues(v => v._1 / v._2).collect().toMap

      if (user_set.isEmpty()) 0
      else user_set.map(x => (x.rating - useravgs(x.user)) / scale(x.rating, useravgs(x.user))).sum() / user_set.count()
  }

  def userpred_spark(ratings: RDD[Rating], usr: Double, itm: Double) : Double = {
    if (ratings.filter(x => (x.user == usr)).isEmpty()) globalavg_spark(ratings)
    else {
      val ru = useravg_spark(ratings, usr)
      val ri = globalavgdev_spark(ratings, itm)

      println("ru ", ru, "ri ", ri)
      
      return ru + ri * scale(ru + ri, ru)
    }
  }

  def MAE_spark(ratings: RDD[Rating], test: RDD[Rating]) : Double = {
    val preds = ratings.map(x => x.user).distinct().collect()
    test.map(x=> scala.math.abs(x.rating - userpred_spark(ratings, x.user, x.item))).sum() / test.count()
  }
  */
}
