package shared

import org.apache.spark.rdd.RDD
// import org.apache.spark.mllib.evaluation.RegressionMetrics

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



  /*----------------------------------------Utils----------------------------------------------------------*/

  def scale(x: Double, useravg: Double): Double = {
    if (x > useravg)
      5 - useravg
    else if (x < useravg)
      useravg - 1
    else
      1
  }

  def MAE(pred: Array[Rating], test : Array[Rating]): Double = {
    //val error = (pred.map(_.rating).toSet -- test.map(_.rating).toSet).map(x => Math.abs(x))
    val errors = 
      for {
        p <- pred
        t <- test
        val error = Math.abs(p.rating-t.rating)/Math.abs(t.rating)
      }yield error

    errors.sum
    
  }

  /*----------------------------------------Baseline----------------------------------------------------------*/
  //1
 def globalavg(ratings : Array[Rating]) : Double = {
    return mean(ratings.map(_.rating).toSeq)
  }

  def predictor_user_avg(ratings : Array[Rating]): (Int, Int) => Double = {
    (user_u: Int, item_i: Int) => 
      if (ratings.filter(x => x.user == user_u).isEmpty)
          globalavg((ratings))
        mean(ratings.filter(x => x.user == user_u).map(_.rating))
  }

  // def user_u_avg(ratings : Array[Rating], user_u : Int) : Double = {
  //   if (ratings.filter(x => x.user == user_u).isEmpty)
  //     globalavg((ratings))
  //   return mean(ratings.filter(x => x.user == user_u).map(_.rating))
  // }
  
  def predictor_item_avg(ratings : Array[Rating]): (Int, Int) => Double = {
    (user_u: Int, item_i: Int) => 
      mean(ratings.filter(x => x.item == 1).map(_.rating))
  }


  def normalizeddev_all(ratings: Array[Rating]) : Array[Rating] = {
    ratings.map(x => Rating(x.user, x.item, (x.rating - predictor_user_avg(ratings)(x.user, x.item)) /
       scale(x.rating, predictor_user_avg(ratings)(x.user, x.item))))
  }

  def dev_avg_item_i(ratings: Array[Rating], item_i: Int) : Double = {  // ratings must be normalized
    mean(ratings.filter(x => x.item == item_i).map(_.rating))
  }


  def predictor_useru_itemi(ratings: Array[Rating]): (Int, Int) => Double = {

    (user_u: Int, item_i: Int) => {
      if (ratings.filter(x => (x.item == item_i) && (x.user == user_u)).isEmpty) 
        predictor_user_avg(ratings)(user_u, item_i)
      else {
        val ru = predictor_user_avg(ratings)(user_u, item_i)
        val ri = dev_avg_item_i(ratings, item_i)
        ru + ri * scale(ri + ru, ru)
      }
    }

  }

  //2
  // def GlobalPred(ratings: Array[Rating]): Array[Rating] = { 
  //   val preds = 
  //     for {
  //       u <- ratings.map(_.user)
  //       i <- ratings.map(_.item)
  //     }yield(Rating(u, i, predictor_useru_itemi(ratings, u, i)))
  //   preds
  // }

  def Global_Avg_MAE(test_ratings: Array[Rating], train_ratings: Array[Rating]): Double = {
    
    val pred = predictor_useru_itemi(train_ratings)
    mean(test_ratings.map(x => scala.math.abs(x.rating - pred(x.user, x.item))))
  }

  // def User_Avg_MAE(test_ratings: Array[Rating], pred: (Int, Int) => Double, GlobalAvg: Double): Double = {
    


  //   val groupbyuser = test_ratings.groupBy(_.user)
  //   groupbyuser.keys.map(u => 
  //   )
  // }
  def User_Avg_MAE(test_ratings: Array[Rating], train_ratings: Array[Rating]): Double = {
    

    val GlobalAvg = globalavg(train_ratings)
    val pred = predictor_useru_itemi(train_ratings)
    val groupbyuser = test_ratings.groupBy(_.user)
    val diff = groupbyuser.keys.map(u => 
      groupbyuser.get(u) match {
      case Some(x) => 
        mean(x.map(y => math.abs(y.rating - pred(y.user, y.item))))
        

      case None => GlobalAvg
      }  
    )
    mean(diff.toSeq)
  }

  def Item_Avg_MAE(test_ratings: Array[Rating], train_ratings: Array[Rating]): Double = {
    
    val GlobalAvg = globalavg(train_ratings)
    val pred = predictor_useru_itemi(train_ratings)
    val groupbyuser = test_ratings.groupBy(_.item)
    val diff = groupbyuser.keys.map(i => 
      groupbyuser.get(i) match {
      case Some(x) => 
        mean(x.map(y => math.abs(y.rating - pred(y.user, y.item))))
        

      case None => GlobalAvg
      }  
    )
    mean(diff.toSeq)
  }

  def Baseline_Avg_MAE(test_ratings: Array[Rating], train_ratings: Array[Rating]): Double = {

    val pred = predictor_useru_itemi(train_ratings)
    mean(test_ratings.map(t => Math.abs(t.rating - pred(t.user, t.item)))) 
  }
  /*----------------------------------------Spark----------------------------------------------------------*/

  /*---------Helpers---------*/

  def standardize(rating: Double, userAvg: Double): Double = {
      (rating - userAvg) / scale(rating, userAvg)
  }

  def compute_usersavg_spark(ratings: RDD[Rating]): Map[Int, Double] = {
    ratings.map(x => (x.user, (x.rating, 1))).reduceByKey((v1, v2) => 
      (v1._1 + v2._1, v1._2 + v2._2)).mapValues(pair => pair._1 / pair._2).collect().toMap
  }

  def compute_itemsavg_spark(ratings: RDD[Rating]): Map[Int, Double] = {
    ratings.map(x => (x.item, (x.rating, 1))).reduceByKey((v1, v2) => 
      (v1._1 + v2._1, v1._2 + v2._2)).mapValues(pair => pair._1 / pair._2).collect().toMap
  }

  def compute_itemsglobaldev_spark(ratings: RDD[Rating], usersAvg: Map[Int, Double]) : Map[Int, Double] = {
    ratings.map(x => (x.item, (standardize(x.rating, usersAvg(x.user)), 1))).reduceByKey((v1, v2) =>
      (v1._1 + v2._1, v1._2 + v2._2)).mapValues(pair => pair._1 / pair._2).collect().toMap
  }

  
  /*---------Predictors---------*/

  def predictor_globalavg_spark(ratings: RDD[Rating]): (Int, Int) => Double = {
    val globalAvg = ratings.map(x => x.rating).mean()
    (u: Int, i: Int) => globalAvg
  }

  def predictor_useravg_spark(ratings: RDD[Rating]): (Int, Int) => Double = {
    
    // Pre-compute global avg
    val globalAvg = predictor_globalavg_spark(ratings)
    
    // Pre-compute users avg
    val usersAvg = compute_usersavg_spark(ratings)

    (u: Int, i: Int) => usersAvg.get(u) match {
      case Some(x) => x
      case None => globalAvg(u, i)
    }
  }
  
  def predictor_itemavg_spark(ratings: RDD[Rating]): (Int, Int) => Double = {
  
    // Pre-compute global avg
    val globalAvg = predictor_globalavg_spark(ratings)

    // Pre-compute users avg
    val usersAvg = compute_usersavg_spark(ratings)

    // Pre-compute items avg
    val itemsAvg = compute_itemsavg_spark(ratings)

    (u: Int, i: Int) => itemsAvg.get(i) match {
      case Some(x) => x
      case None => usersAvg.get(u) match {
        case Some(x) => x
        case None => globalAvg(u, i)
      }
    }
  }

  def predictor_rating_spark(ratings: RDD[Rating]): (Int, Int) => Double = {

    val globalAvg = predictor_globalavg_spark(ratings)(1,1)

    // Pre compute user avgs
    val usersAvg = compute_usersavg_spark(ratings)
    
    // Pre compute global avg devs
    val globalAvgDevs = compute_itemsglobaldev_spark(ratings, usersAvg)

    (u: Int, i: Int) =>  {
      val ru = usersAvg.get(u) match {
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


  /*----------------------------------------Personalized----------------------------------------------------------*/

  // def preprocess_ratings(normalized_ratings: Array[Rating]): Array[Rating] = {

  //   val groupbyuser = normalized_ratings.groupBy(_.user)

  //   normalized_ratings.map(x => Rating(x.user, x.item, 
  //     x.rating/Math.sqrt(groupbyuser.get(x.user) match{
  //       case Some(x_user) => x_user.map(user_rating => Math.pow(user_rating.rating, 2)).sum
  //       case None => 0
  //     })))
  // }

  // def similarity_u_v(prep_ratings: Array[Rating], u: Int, v: Int): Double = {

  //   val prep_ratings_gb = prep_ratings.groupBy(_.user)
  //   val prep_ratings_u = prep_ratings_gb.getOrElse(u, 0) //Any becasue Rating or Int
  //   val prep_ratings_v = prep_ratings_gb.getOrElse(v, 0)

  //   prep_ratings_u.filter(r => prep_ratings_v.map(_.item).contains(r.item)).map(u => 
  //     u.rating * prep_ratings_v.filter(v => v.item == u.item).rating)
  // }

  // def weighted_dev_avg_item_i(normalized_ratings: Array[Rating], item_i: Int): Array[Rating] = {
  //   val groupbyuser = normalized_ratings.groupBy(_.item)
  //   val prepRating = preprocess_ratings(normalized_ratings)

  //   normalized_ratings.map(x => Rating(x.user, x.item, 
  //     groupbyuser.get(x.item) match {
  //       case Some(x_item) => x_item.map(item_rating => item_rating.rating * 
  //         mean(prepRating.filter(y => y.user == x_item.map(_.user) && y.item == x_item.map(_.item)).map(_.rating))
  //       ).sum / x_item.map(item_rating => Math.abs(prepRating.filter(y => y.user == x_item.map(_.user) && y.item == x_item.map(_.item))))

  //       case None => 0 
  //     }
  //   ))
  // }

  // def predictor_useru_itemi_personalized(ratings: Array[Rating]): (Int, Int) => Double = {

  //   (user_u: Int, item_i: Int) => {
  //     if (ratings.filter(x => (x.item == item_i) && (x.user == user_u)).isEmpty) 
  //       predictor_user_avg(ratings)(user_u, item_i)
  //     else {
  //       val ru = predictor_user_avg(ratings)(user_u, item_i)
  //       val ri = dev_avg_item_i(ratings, item_i)
  //       ru + ri * scale(ri + ru, ru)
  //     }
  //   }

  // }


}