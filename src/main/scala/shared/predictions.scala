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
    * Computes global average
    * 
    * @param ratings
    * @return Global average
    */
  def computeGlobalAvg(ratings: Array[Rating]): Double = {
    mean(ratings.map(x => x.rating))
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
    ratings.groupBy(_.item).mapValues(x => mean(x.map(y => y.rating)))
  }

  /**
    * Computes global average deviation of each item 
    * 
    * @param ratings
    * @param users_avg dictionary of user avegage ratings (user, avg-rating)
    * @return Map with key-value pairs (item, global average dev)
    */
  def computeItemsGlobalDev(standardized_ratings: Array[Rating], users_avg: Map[Int, Double]): Map[Int, Double] = {
    standardized_ratings.groupBy(_.item).mapValues(x => mean(x.map(y => y.rating)))
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
  
  /*----------------------------------------Global variables----------------------------------------------------------*/

  var global_avg: Double = 0.0
  var users_avg: Map[Int,Double] = null
  var items_avg: Map[Int, Double] = null
  var global_avg_devs: Map[Int, Double] = null
  var standardized_ratings: Array[Rating] = null
  var similarities_uniform: Map[(Int, Int),Double] = null
  var similarities_cosine: Map[(Int, Int),Double] = null
  var preprocessed_ratings: Array[Rating] = null
  var user_similarities: Map[Int,Array[(Int, Double)]] = null
  var preprocessed_groupby_user: Map[Int,Array[Rating]] = null
  var standardized_groupby_item: Map[Int,Array[Rating]] = null

  /*----------------------------------------Baseline----------------------------------------------------------*/
  
  /**
    * Predictor using the global average
    *
    * @param ratings
    * @return a predictor
    */
  def predictorGlobalAvg(ratings: Array[Rating]): (Int, Int) => Double = {
    //val global_avg = mean(ratings.map(x => x.rating))
    (u: Int, i: Int) => global_avg
  }
  
  /**
    * Predictor using the user average
    *
    * @param ratings
    * @return a predictor
    */
  def predictorUserAvg(ratings: Array[Rating]): (Int, Int) => Double = {

    (u: Int, i: Int) => users_avg.get(u) match {
      case Some(x) => x
      case None => global_avg
    }
  }
  
  /**
    * Predictor using the item average
    *
    * @param ratings
    * @return a predictor
    */
  def predictorItemAvg(ratings: Array[Rating]): (Int, Int) => Double = {

    (u: Int, i: Int) => items_avg.get(i) match {
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

    (u: Int, i: Int) =>  {

      users_avg.get(u) match {
        case Some(x) => {
          val ri = global_avg_devs.get(i) match {
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
    * Computes global average
    * 
    * @param ratings
    * @return Global average
    */
  def computeGlobalAvg(ratings: RDD[Rating]): Double = {
    ratings.map(x => x.rating).mean()
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
    //val global_avg = ratings.map(x => x.rating).mean()
    (u: Int, i: Int) => global_avg
  }
 
  /**
    * Predictor using the user average
    *
    * @param ratings
    * @return a predictor
    */
  def predictorUserAvg(ratings: RDD[Rating]): (Int, Int) => Double = {

    (u: Int, i: Int) => users_avg.get(u) match {
      case Some(x) => x
      case None => global_avg
    }
  }
  
  /**
    * Predictor using the item average
    *
    * @param ratings
    * @return a predictor
    */
  def predictorItemAvg(ratings: RDD[Rating]): (Int, Int) => Double = {
  

    (u: Int, i: Int) => items_avg.get(i) match {
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

    (u: Int, i: Int) =>  {
      users_avg.get(u) match {
        case Some(x) => {
          val ri = global_avg_devs.get(i) match {
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
    val user_set = preprocessed_groupby_user.keySet

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

    (u: Int, i: Int) =>  {

      // Faster if pre-compute everything ?
      val ratings_i = standardized_ratings.filter(x => x.item == i)

      val ri_numerator = if (ratings_i.isEmpty) 0.0 else ratings_i.map(x => {
        similarities_uniform.get((x.user, u)) match {
          case Some(y) => y * x.rating 
          case None => 0.0
        }
      }).sum

      val ri_denominator =  ratings_i.map(x => {
        similarities_uniform.get((x.user, u)) match {
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
  def adjustedCosine(groupby_user: Map[Int,Array[Rating]], u: Int, v: Int): Double = {
    if (u == v) 1.0
    else {
      val ratings_u_v = groupby_user(u) ++ groupby_user(v)
      ratings_u_v.groupBy(_.item).filter(x => x._2.length == 2).mapValues(x => x.foldLeft(1.0)((mult, rating) => mult * rating.rating)).values.sum
    }
  }

  /**
    * Computes the cosine similarity for all pairs of users
    *
    * @param preprocessed_ratings
    * @return a dictionary of key-value pairs ((user1, user2), similarity)
    */
  def computeCosine(preprocessed_ratings: Array[Rating]): Map[(Int, Int), Double] = {

   val user_set = preprocessed_groupby_user.keySet

   val user_pairs = (for(u <- user_set; v <- user_set if u < v) yield (u, v))

   //val groupby_user = preprocessed_ratings.groupBy(_.user)
   
   user_pairs.map(x => (x, adjustedCosine(preprocessed_groupby_user, x._1, x._2))).toMap
  }

  /**
    * Computes the user-specific weighted sum deviation using the cosine similarity
    *
    * @param cosine_similarities
    * @param usr the user
    * @param itm the item
    * @return the user-specific weighted sum deviation of item itm for user usr
    */
  def computeRiCosine(cosine_similarities: Map[(Int, Int), Double], usr: Int, itm: Int): Double = {
    
    val ratings_i = standardized_groupby_item.getOrElse(itm, Array[Rating]())
    
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

    (u: Int, i: Int) =>  {

      users_avg.get(u) match {
        case Some(x) => {
          val ri = computeRiCosine(similarities_cosine, u, i)
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
 
    val user_set = preprocessed_groupby_user.keySet

    val user_pairs = (for(u <- user_set; v <- user_set if u < v) yield (u, v))
    
    user_pairs.map(x => (x, jaccard(preprocessed_groupby_user(x._1).map(_.item).toSet, preprocessed_groupby_user(x._2).map(_.item).toSet))).toMap
  }

  
  /**
    * Computes the user-specific weighted sum deviation using the jaccard similarity
    *
    * @param jaccard_similarities
    * @param usr the user
    * @param itm the item
    * @return the user-specific weighted sum deviation of item itm for user usr
    */
  def computeRiJaccard(jaccard_similarities: Map[(Int, Int), Double], usr: Int, itm: Int): Double = {
    
    val ratings_i = standardized_groupby_item.getOrElse(itm, Array[Rating]())
    
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

    val jaccard_Map = jaccardSimilarityAllUsers(preprocessed_ratings)

    (u1: Int, u2: Int) =>  {
      val ru = users_avg.get(u1) match {
        case Some(x) => x
        case None => global_avg
      }

      val ri = computeRiJaccard(jaccard_Map, u1, u2)
      ru + ri * scale(ru + ri, ru)
    }
  }
  

  /*---------------------------------------Neighbourhood-based---------------------------------------------------------*/
  
  def computeUserSimilarities(ratings: Array[Rating]): Map[Int,Array[(Int, Double)]] = {
    val user_set = preprocessed_groupby_user.keySet
    user_set.map(x => {
      (x , (for (u <- user_set if (u != x)) yield (u, if (x < u) similarities_cosine((x, u)) else similarities_cosine((u, x)))).toArray.sortBy(-_._2))
    }).toMap
  }
  /**
    * Predictor for any k-nearest neighboors
    *
    * @param ratings
    * @return a predictor for any k
    */
  def predictorAllNN(ratings: Array[Rating]): Int => (Int, Int) => Double = {

    (k: Int) => {

      val k_user_similarities = user_similarities.mapValues(x => x.slice(0, k))
      
        (u: Int, i: Int) =>  {

          users_avg.get(u) match {
            case Some(ru) => {

              // Similarity values of k similar users to u
              val k_user_similar_map = k_user_similarities(u).toMap

              // Ratings of item i by k similar users
              val rating_i_k_users = standardized_groupby_item.getOrElse(i, Array[Rating]()).filter(x => k_user_similar_map.keySet.contains(x.user))
              
              // Keep among the k similar users those who have rated i
              val k_users_rating_i = rating_i_k_users.map(x => x.user).distinct.map(x => (x, k_user_similar_map(x))).toMap

              val numerator = if (rating_i_k_users.isEmpty) 0.0 else rating_i_k_users.map(x => x.rating * k_users_rating_i(x.user)).sum
              val denominator = if (rating_i_k_users.isEmpty) 0.0 else k_users_rating_i.mapValues(x => scala.math.abs(x)).values.sum

              val ri = if (denominator == 0.0) 0.0 else numerator / denominator

              ru + ri * scale(ru + ri, ru)

            }

            case None => global_avg
          }
            
        }
    }
  }

  /*---------------------------------------Neighbourhood-based---------------------------------------------------------*/
    
  def recommendMovies(ratings: Array[Rating], predictor: ((Int, Int) => Double), usr: Int, n: Int): Array[(Int, Double)] = {

      val movies_not_rated = ratings.filter(x => x.user != usr).map(x => x.item).distinct

      val preds = movies_not_rated.map(x => (x, predictor(usr, x))).sortBy(x => (-x._2, x._1))
      
      preds.slice(0, n)
  }

}