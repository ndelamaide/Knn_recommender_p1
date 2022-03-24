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

  /** 
    * Computes average rating of every user in ratings
    * 
    * @param ratings
    * @return Map with key-value pairs (user, avg-rating)
    */
  def computeUsersAvg(ratings: Array[Rating]): Map[Int, Double] = {
    ratings.map(x => (x.user, x.rating)).groupBy(_._1).mapValues(x => mean(x.map(y => y._2)))
  }

  /** 
    * Computes average rating of every item in ratings
    * 
    * @param ratings
    * @return Map with key-value pairs (item, avg-rating)
    */
  def computeItemsAvg(ratings: Array[Rating]): Map[Int, Double] = {
    ratings.map(x => (x.item, x.rating)).groupBy(_._1).mapValues(x => mean(x.map(y => y._2)))
  }

  /**
    * Computes global average deviation of each item 
    * 
    * @param ratings
    * @param users_avg dictionary of user avegage ratings (user, avg-rating)
    * @return Map with key-value pairs (item, global average dev)
    */
  def computeItemsGlobalDev(ratings: Array[Rating], users_avg: Map[Int, Double]): Map[Int, Double] = {
    ratings.map(x => (x.item, standardize(x.rating, users_avg(x.user)))).groupBy(_._1).mapValues(x => mean(x.map(y => y._2)))
  }
  
  /** 
    * Standardizes all ratings in ratings dataframe 
    * 
    * @param ratings
    * @param users_avg dictionary of user avegage ratings (user, avg-rating)
    * @return Dataframe with standardized ratings
    */
  def standardizeRatings(ratings: Array[Rating], users_avg: Map[Int, Double]): Array[Rating] = {
     ratings.map(x => Rating(x.user, x.item, standardize(x.rating, users_avg(x.user))))
   }
  
  /** 
    * Computes the MAE of a given predictor
    * 
    * @param test_ratings ratings to compute the MAE on
    * @param predictor the predictor used to make the predictions
    * @return the value of the MAE
    */
  def MAE(test_ratings: Array[Rating], predictor: (Int, Int) => Double): Double = {
    mean(test_ratings.map(x => scala.math.abs(x.rating - predictor(x.user, x.item))))
  }

  /*----------------------------------------Baseline----------------------------------------------------------*/
  
  /**
    * Predictor using the global average
    *
    * @param ratings
    * @return a predictor
    */
  def predictorGlobalAvg(ratings: Array[Rating]): (Int, Int) => Double = {
    val global_avg = mean(ratings.map(x => x.rating))
    (u: Int, i: Int) => global_avg
  }
  
  /**
    * Predictor using the user average
    *
    * @param ratings
    * @return a predictor
    */
  def predictorUserAvg(ratings: Array[Rating]): (Int, Int) => Double = {
    
    // Pre-compute global avg
    val global_avg = predictorGlobalAvg(ratings)
    
    // Pre-compute users avg
    val users_avg = computeUsersAvg(ratings)

    (u: Int, i: Int) => users_avg.get(u) match {
      case Some(x) => x
      case None => global_avg(u, i)
    }
  }
  
  /**
    * Predictor using the item average
    *
    * @param ratings
    * @return a predictor
    */
  def predictorItemAvg(ratings: Array[Rating]): (Int, Int) => Double = {
  
    // Pre-compute global avg
    val global_avg = predictorGlobalAvg(ratings)(1, 1)

    // Pre-compute users avg
    val users_avg = computeUsersAvg(ratings)

    // Pre-compute items avg
    val itemsAvg = computeItemsAvg(ratings)

    (u: Int, i: Int) => itemsAvg.get(i) match {
      case Some(x) => x
      case None => users_avg.get(u) match {
        case Some(x) => x
        case None => global_avg
      }
    }
  }

  /**
    * Predictor using the baseline
    *
    * @param ratings
    * @return a predictor
    */
  def predictorRating(ratings: Array[Rating]): (Int, Int) => Double = {

    val global_avg = predictorGlobalAvg(ratings)(1,1)

    // Pre compute user avgs
    val users_avg = computeUsersAvg(ratings)
    
    // Pre compute global avg devs
    val globalAvgDevs = computeItemsGlobalDev(ratings, users_avg)

    (u: Int, i: Int) =>  {

      users_avg.get(u) match {
        case Some(x) => {
          val ri = globalAvgDevs.get(i) match {
            case Some(x) => x
            case None => 0.0
          }
          x + ri * scale(x + ri, x)
        }
        case None => global_avg
      }
    }
  }

  /*----------------------------------------Spark----------------------------------------------------------*/

  /*---------Helpers---------*/

  def standardize(rating: Double, userAvg: Double): Double = {
      (rating - userAvg) / scale(rating, userAvg)
  }
  
  /** 
    * Computes average rating of every user in ratings
    * 
    * @param ratings
    * @return Map with key-value pairs (user, avg-rating)
    */
  def computeUsersAvg(ratings: RDD[Rating]): Map[Int, Double] = {
    ratings.map(x => (x.user, (x.rating, 1))).reduceByKey((v1, v2) => 
      (v1._1 + v2._1, v1._2 + v2._2)).mapValues(pair => pair._1 / pair._2).collect().toMap
  }
  
  /** 
    * Computes average rating of every item in ratings
    * 
    * @param ratings
    * @return Map with key-value pairs (item, avg-rating)
    */
  def computeItemsAvg(ratings: RDD[Rating]): Map[Int, Double] = {
    ratings.map(x => (x.item, (x.rating, 1))).reduceByKey((v1, v2) => 
      (v1._1 + v2._1, v1._2 + v2._2)).mapValues(pair => pair._1 / pair._2).collect().toMap
  }
  
  /**
    * Computes global average deviation of each item 
    * 
    * @param ratings
    * @param users_avg dictionary of user avegage ratings (user, avg-rating)
    * @return Map with key-value pairs (item, global average dev)
    */
  def computeItemsGlobalDev(ratings: RDD[Rating], users_avg: Map[Int, Double]) : Map[Int, Double] = {
    ratings.map(x => (x.item, (standardize(x.rating, users_avg(x.user)), 1))).reduceByKey((v1, v2) =>
      (v1._1 + v2._1, v1._2 + v2._2)).mapValues(pair => pair._1 / pair._2).collect().toMap
  }

  /** 
    * Computes the MAE of a given predictor
    * 
    * @param test_ratings ratings to compute the MAE on
    * @param predictor the predictor used to make the predictions
    * @return the value of the MAE
    */
  def MAE(test_ratings: RDD[Rating], predictor: (Int, Int) => Double): Double = {
      test_ratings.map(x => scala.math.abs(x.rating - predictor(x.user, x.item))).mean()
  }

  /*---------Predictors---------*/
  
  /**
    * Predictor using the global average
    *
    * @param ratings
    * @return a predictor
    */
  def predictorGlobalAvg(ratings: RDD[Rating]): (Int, Int) => Double = {
    val global_avg = ratings.map(x => x.rating).mean()
    (u: Int, i: Int) => global_avg
  }
 
  /**
    * Predictor using the user average
    *
    * @param ratings
    * @return a predictor
    */
  def predictorUserAvg(ratings: RDD[Rating]): (Int, Int) => Double = {
    
    // Pre-compute global avg
    val global_avg = predictorGlobalAvg(ratings)
    
    // Pre-compute users avg
    val users_avg = computeUsersAvg(ratings)

    (u: Int, i: Int) => users_avg.get(u) match {
      case Some(x) => x
      case None => global_avg(u, i)
    }
  }
  
  /**
    * Predictor using the item average
    *
    * @param ratings
    * @return a predictor
    */
  def predictorItemAvg(ratings: RDD[Rating]): (Int, Int) => Double = {
  
    // Pre-compute global avg
    val global_avg = predictorGlobalAvg(ratings)(1, 1)

    // Pre-compute users avg
    val users_avg = computeUsersAvg(ratings)

    // Pre-compute items avg
    val itemsAvg = computeItemsAvg(ratings)

    (u: Int, i: Int) => itemsAvg.get(i) match {
      case Some(x) => x
      case None => users_avg.get(u) match {
        case Some(y) => y
        case None => global_avg
      }
    }
  }

  /**
    * Predictor using the baseline
    *
    * @param ratings
    * @return a predictor
    */
  def predictorRating(ratings: RDD[Rating]): (Int, Int) => Double = {

    val global_avg = predictorGlobalAvg(ratings)(1,1)

    // Pre compute user avgs
    val users_avg = computeUsersAvg(ratings)
    
    // Pre compute global avg devs
    val globalAvgDevs = computeItemsGlobalDev(ratings, users_avg)

    (u: Int, i: Int) =>  {
      users_avg.get(u) match {
        case Some(x) => {
          val ri = globalAvgDevs.get(i) match {
            case Some(x) => x
            case None => 0.0
          }
          x + ri * scale(x + ri, x)
        }
        case None => global_avg
      }
    }
  }

  /*----------------------------------------Personalized----------------------------------------------------------*/


  /**
    * Pre-process the ratings before computing the similarity
    *
    * @param standardized_ratings
    * @return a dataframe with the ratings pre-processed
    */
  def preprocessRatings(standardized_ratings: Array[Rating]): Array[Rating] = {

    // Compute sum of square of devs for each user
    val squared_sum_users = standardized_ratings.groupBy(_.user).mapValues(x => x.foldLeft(0.0)((sum, rating) => sum + scala.math.pow(rating.rating, 2)))

    standardized_ratings.map(x => Rating(x.user, x.item, x.rating / scala.math.sqrt(squared_sum_users(x.user))))
  }
  
  /**
    * Computes the similarity between each user using a value of 1 (uniform)
    *
    * @param ratings
    * @return a dictionary of key-value pairs ((user1, user2), similarity)
    */
  def computeSimilaritiesUniform(ratings: Array[Rating]): Map[(Int, Int), Double] = {
    val user_set = ratings.map(x => x.user).distinct

    val sims = for {
      u1 <- user_set
      u2 <- user_set
    } yield ((u1, u2), 1.0)

    sims.toMap
  }

  /**
    * Predictor using a similarity of 1 between each user (uniform)
    *
    * @param ratings
    * @return a predictor
    */
  def predictorUniform(ratings: Array[Rating]): (Int, Int) => Double = {

    val global_avg = predictorGlobalAvg(ratings)(-1,-1)

    val users_avg = computeUsersAvg(ratings)

    val standardized_ratings = standardizeRatings(ratings, users_avg)

    val similarities = computeSimilaritiesUniform(ratings)

    (u: Int, i: Int) =>  {

      // Faster if pre-compute everything ?
      val ratings_i = standardized_ratings.filter(x => x.item == i)

      val ri_numerator = if (ratings_i.isEmpty) 0.0 else ratings_i.map(x => {
        similarities.get((x.user, u)) match {
          case Some(y) => y * x.rating 
          case None => 0.0
        }
      }).sum

      val ri_denominator =  ratings_i.map(x => {
        similarities.get((x.user, u)) match {
          case Some(y) => y
          case None => 0.0
        }
      }).sum

      users_avg.get(u) match {
        case Some(x) => {
          val ri = if (ri_denominator == 0.0) 0.0 else ri_numerator / ri_denominator
          x + ri * scale(x + ri, x)
        }
        case None => global_avg
      }
    }
  }

  /**
    * Computes the adjusted cosine similarity between two users
    *
    * @param preprocessed_ratings
    * @param u first user
    * @param v secoond user
    * @return the cosine similarity between the two users
    */
  def adjustedCosine(preprocessed_ratings: Array[Rating], u: Int, v: Int): Double = {
    preprocessed_ratings.filter(x => (x.user == u) || (x.user == v)).groupBy(_.item).filter(x => x._2.length == 2).mapValues(x => x.foldLeft(1.0)((mult, rating) => mult * rating.rating)).values.sum
  }

  /**
    * Computes the cosine similarity for all pairs of users
    *
    * @param preprocessed_ratings
    * @return a dictionary of key-value pairs ((user1, user2), similarity)
    */
  def computeCosine(preprocessed_ratings: Array[Rating]): Map[(Int, Int), Double] = {

   val user_set = preprocessed_ratings.map(x => x.user).distinct

   val user_pairs = (for(u <- user_set; v <- user_set if u < v) yield (u, v)).distinct
   
   user_pairs.map(x => (x, adjustedCosine(preprocessed_ratings, x._1, x._2))).toMap
  }

  /**
    * Computes the user-specific weighted sum deviation using the cosine similarity
    *
    * @param standardized_ratings
    * @param cosine_similarities
    * @param usr the user
    * @param itm the item
    * @return the user-specific weighted sum deviation of item itm for user usr
    */
  def computeRiCosine(standardized_ratings: Array[Rating], cosine_similarities: Map[(Int, Int), Double], usr: Int, itm: Int): Double = {
    
    val ratings_i = standardized_ratings.filter(x => x.item == itm)
    
    val similarities = ratings_i.map(x => {
      if (x.user == usr) (x.user, 1.0)
      else cosine_similarities.get(if (x.user < usr) (x.user, usr) else (usr, x.user)) match {
        case Some(y) => (x.user, y)
        case None => (x.user, 0.0)
      }
    }).toMap

    val numerator = if (ratings_i.isEmpty) 0.0 else ratings_i.map(x => similarities(x.user) * x.rating).sum

    val denominator = if (similarities.isEmpty) 0.0 else similarities.mapValues(x => scala.math.abs(x)).values.sum

    if (denominator == 0.0) 0.0 else numerator / denominator
  }

  /**
    * Predictor using the cosine similarity
    *
    * @param ratings
    * @return a predictor
    */
  def predictorCosine(ratings: Array[Rating]): (Int, Int) => Double = {

    val global_avg = predictorGlobalAvg(ratings)(-1,-1)

    val users_avg = computeUsersAvg(ratings)

    val standardized_ratings = standardizeRatings(ratings, users_avg)

    val preprocessed_ratings =  preprocessRatings(standardized_ratings)

    val cosine_similarities = computeCosine(preprocessed_ratings)

    (u: Int, i: Int) =>  {

      users_avg.get(u) match {
        case Some(x) => {
          val ri = computeRiCosine(standardized_ratings, cosine_similarities, u, i)
          x + ri * scale(x + ri, x)
        }
        case None => global_avg
      }
    }
  }
  /*---------------------------------------Jaccard-Similarity---------------------------------------------------------*/


  /**
    * Returns a function that returns all movies rated by a specific user
    *
    * @param u Specific user
    * @return  function that returns all movies rated by a specific user
    */

  def mapmovies(ratings: Array[Rating]): Int => Array[Int] = {
    (u: Int) => 
      ratings.filter(x => x.user == u).map(_.item)
  }

  /**
    * Computes the adjusted cosine similarity between two users
    *
    * @param a Movie set corresponding to the first viewer
    * @param b Movie set corresponding to the second viewer
    * @return the jaccard similarity between the two users
    */
  def jaccard[A](a: Set[A], b: Set[A]): Double = {
    /*
    Given two sets, compute its Jaccard similarity and return its result.
    If the union part is zero, then return 0.
    */
    if (a.isEmpty || b.isEmpty){return 0.0}
    a.intersect(b).size.toFloat/a.union(b).size.toFloat

  }

  /**
    * Computes the Jaccard similarity for user pair (1, 2)
    *
    * @param preprocessed_ratings
    * @return the similarity between user 1 and 2
    */
  def jaccardpred12(preprocessed_ratings: Array[Rating]): Double = {
    val movies1 = preprocessed_ratings.filter(x => x.user == 1).map(_.item)
    val movies2 = preprocessed_ratings.filter(x => x.user == 2).map(_.item)


    jaccard(movies1.toSet, movies2.toSet)
  }

  /**
    * Computes the Jaccard similarity for all pairs of users
    *
    * @param preprocessed_ratings
    * @return a dictionary of key-value pairs ((user1, user2), similarity)
    */
  def jaccardSimilarityAllUsers(preprocessed_ratings: Array[Rating]): Map[(Int, Int), Double] = {
 
    val user_set = preprocessed_ratings.map(x => x.user).distinct

    val user_pairs = (for(u <- user_set; v <- user_set if u < v) yield (u, v)).distinct

    val moviesmap = mapmovies(preprocessed_ratings)
    
    user_pairs.map(x => (x, jaccard(moviesmap(x._1).toSet, moviesmap(x._2).toSet))).toMap

  }

  
  /**
    * Computes the user-specific weighted sum deviation using the jaccard similarity
    *
    * @param standardized_ratings
    * @param jaccard_similarities
    * @param usr the user
    * @param itm the item
    * @return the user-specific weighted sum deviation of item itm for user usr
    */
  def computeRiJaccard(standardized_ratings: Array[Rating], jaccard_similarities: Map[(Int, Int), Double], usr: Int, itm: Int): Double = {
    
    val ratings_i = standardized_ratings.filter(x => x.item == itm)
    
    val similarities = ratings_i.map(x => {
      if (x.user == usr) (x.user, 1.0)
      else jaccard_similarities.get(if (x.user < usr) (x.user, usr) else (usr, x.user)) match {
        case Some(y) => (x.user, y)
        case None => (x.user, 0.0)
      }
    }).toMap

    val numerator = if (ratings_i.isEmpty) 0.0 else ratings_i.map(x => similarities(x.user) * x.rating).sum

    val denominator = if (similarities.isEmpty) 0.0 else similarities.mapValues(x => scala.math.abs(x)).values.sum

    if (denominator == 0.0) 0.0 else numerator / denominator
  }


    /**
  * Predictor using the Jaccard similarity
  *
  * @param ratings
  * @return a predictor
  */
  def predictorJaccard(ratings: Array[Rating]): (Int, Int) => Double = {

    val global_avg = predictorGlobalAvg(ratings)(-1,-1)

    val users_avg = computeUsersAvg(ratings)

    val standardized_ratings = standardizeRatings(ratings, users_avg)

    val preprocessed_ratings =  preprocessRatings(standardized_ratings)

    val jaccard_Map = jaccardSimilarityAllUsers(preprocessed_ratings)

    (u1: Int, u2: Int) =>  {
      val ru = users_avg.get(u1) match {
        case Some(x) => x
        case None => global_avg
      }

      val ri = computeRiJaccard(standardized_ratings, jaccard_Map, u1, u2)
      ru + ri * scale(ru + ri, ru)
    }
  }
  

  /*---------------------------------------Neighbourhood-based---------------------------------------------------------*/
  
    /**
    * Computes the user-specific weighted-sum deviation using only the k-nearest neighboors
    *
    * @param standardized_ratings
    * @param cosine_similarities
    * @param usr the user
    * @param itm the item
    * @param k the k-nearest neighboors to consider
    * @return the user-specific weighted-sum deviation for item itm and user usr
    */
  def computeRikNN(standardized_ratings: Array[Rating], cosine_similarities: Map[(Int, Int), Double], usr: Int, itm: Int, k: Int): Double = {
    
    val ratings_i = standardized_ratings.filter(x => x.item == itm)
    
    // Don't compute self-similarity
    val similarities = ratings_i.map(x => cosine_similarities.get(if (x.user < usr) (x.user, usr) else (usr, x.user)) match {
        case Some(y) => (x.user, y)
        case None => (x.user, 0.0)
    }).sortBy(_._2)(Ordering[Double].reverse).slice(0, k).toMap

    val numerator = if (ratings_i.isEmpty) 0.0 else ratings_i.map(x => similarities.get(x.user) match {
      case Some(y) => y * x.rating
      case None => 0.0 
    }).sum

    val denominator = if (similarities.isEmpty) 0.0 else similarities.mapValues(x => scala.math.abs(x)).values.sum

    if (denominator == 0.0) 0.0 else numerator / denominator
  }

  
  /**
    * Predictor for any k-nearest neighboors
    *
    * @param ratings
    * @return a predictor for any k
    */
  // def predictorAllNN(ratings: Array[Rating]): Int => (Int, Int) => Double = {

  //   val global_avg = predictorGlobalAvg(ratings)(-1,-1)

  //   val users_avg = computeUsersAvg(ratings)

  //   val standardized_ratings = standardizeRatings(ratings, users_avg)

  //   val preprocessed_ratings =  preprocessRatings(standardized_ratings)

  //   val user_set = preprocessed_ratings.map(x => x.user).distinct

  //   val user_pairs_cosine = for(u <- user_set; v <- user_set if u != v) yield (u, (v, adjustedCosine(standardized_ratings, u, v)))
    
  //   (k: Int) => {

  //     val user_other_similarities = user_pairs_cosine.groupBy(_._1).mapValues(x => x.map(y => y._2).sortBy(-_._2).zipWithIndex.map(y => if (y._2 < k) y._1 else (y._1._1, 0.0)))

  //     println("K", k)
      
  //       (u: Int, i: Int) =>  {

  //       users_avg.get(u) match {
  //         case Some(ru) => {
            
  //           val k_similarities = user_other_similarities(u).filter(x => x._2 != 0.0).toMap

  //           if (k_similarities.isEmpty) ru else {

  //             val items_i_k_similar = preprocessed_ratings.filter(x => (x.item == i) && k_similarities.isDefinedAt(x.user))

  //             val numerator = items_i_k_similar.map(x => x.rating * k_similarities(x.user)).sum
  //             val denominator = k_similarities.mapValues(x => scala.math.abs(x)).values.sum

  //             val ri = if (denominator == 0.0) 0.0 else numerator / denominator

  //             ru + ri * scale(ru + ri, ru)
  //           }          
  //         }
  //         case None => global_avg
  //       }
  //     }
  //   }
  // }

  /**
    * Predictor for any k-nearest neighboors
    *
    * @param ratings
    * @return a predictor for any k
    */
  def predictorAllNN(ratings: Array[Rating]): Int => (Int, Int) => Double = {

    val global_avg = predictorGlobalAvg(ratings)(-1,-1)

    val users_avg = computeUsersAvg(ratings)

    val standardized_ratings = standardizeRatings(ratings, users_avg)

    val preprocessed_ratings =  preprocessRatings(standardized_ratings)

    val cosine_similarities = computeCosine(preprocessed_ratings)

    k: Int => (u: Int, i: Int) =>  {

      users_avg.get(u) match {
        case Some(x) => {
          val ri = computeRikNN(standardized_ratings, cosine_similarities, u, i, k)
          x + ri * scale(x + ri, x)
        }
        case None => global_avg
      }
    }
  }

  /*---------------------------------------Neighbourhood-based---------------------------------------------------------*/
    
  def recommendMovies(ratings: Array[Rating], predictor: ((Int, Int) => Double), usr: Int, n: Int): Array[(Int, Double)] = {

      val movies_not_rated = ratings.filter(x => x.user != usr).map(x => x.item).distinct

      val preds = movies_not_rated.map(x => (x, predictor(usr, x))).sortBy(x => (-x._2, x._1))

      println("preds", preds.mkString(" "))
      
      preds.slice(0, n)
  }

}