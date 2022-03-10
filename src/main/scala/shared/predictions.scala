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
  
  def predictor_globalavg_spark(ratings: RDD[Rating]): (Int, Int) => Double = {
    val globalAvg = ratings.map(x => x.rating).mean()
    (u: Int, i: Int) => globalAvg
  }
  
  // Only used to predict user 1 avg
  def predictor_useravg_spark(ratings: RDD[Rating]): (Int, Int) => Double = {
    
    // Pre-compute global avg
    //val globalAvg = predictor_globalavg_spark(ratings)
    
    /*
    // Pre-compute users avg
    val userAvgs = ratings.map(x => x.user).distinct().collect().map(usr => 
      (usr, ratings.filter(x => x.user == usr).map(x => x.rating).mean())).toMap

    (u: Int, i: Int) => userAvgs.get(u) match {
      case Some(x) => x
      case None => globalAvg(u, i)
    } */
    
    (u: Int, i: Int) => ratings.filter(x => x.user == u).map(x => x.rating).mean()
  }
  
  // Only used to predict item 1 avg
  def predictor_itemavg_spark(ratings: RDD[Rating]): (Int, Int) => Double = {
  /*
    // Pre-compute global avg
    val globalAvg = predictor_globalavg_spark(ratings)

    // Pre-compute users avg
    val userAvgs = ratings.map(x => x.user).distinct().collect().map(usr => 
      (usr, ratings.filter(x => x.user == usr).map(x => x.rating).mean())).toMap

    // Pre-compute items avg
    val itemAvgs = ratings.map(x => x.item).distinct().collect().map(itm => 
      (itm, ratings.filter(x => x.item == itm).map(x => x.rating).mean())).toMap

    (u: Int, i: Int) => itemAvgs.get(i) match {
      case Some(x) => x
      case None => userAvgs.get(u) match {
        case Some(x) => x
        case None => globalAvg(u, i)
      }
    } */
    
    (u: Int, i: Int) => ratings.filter(x => x.item == i).map(x => x.rating).mean()
  }

  def predictor_rating_spark(ratings: RDD[Rating]): (Int, Int) => Double = {

    val globalAvg = predictor_globalavg_spark(ratings)(1,1)

    // Pre compute user avgs
    val predictorUseravg = predictor_useravg_spark(ratings)
    val userAvgs = ratings.map(x => x.user).distinct().collect().map(usr => (usr, predictorUseravg(usr, 1))).toMap

    // Standardize ratings
    val ratingsStrandard = ratings.map(x => Rating(x.user, x.item, (x.rating - userAvgs(x.user)) / scale(x.rating, userAvgs(x.user))))

    // Compute global avg deviation of each item
    val globalAvgDevs = ratings.map(x => x.item).distinct().collect().map(itm => 
      (itm, ratingsStrandard.filter(x => x.item == itm).map(x => x.rating).mean())).toMap

    (u: Int, i: Int) =>  {
      val ru = userAvgs.get(u) match {
        case Some(x) => x
        case None => globalAvg
      }
      val ri = globalAvgDevs.get(i) match {
        case Some(x) => x
        case None => 0.0
      }

      ru + ri * scale(ru + ri, ru)
    }
  }
  
  def MAE_spark(test_ratings: RDD[Rating], predictor: (Int, Int) => Double): Double = {
      test_ratings.map(x => scala.math.abs(x.rating - predictor(x.user, x.item))).mean()
  }
}