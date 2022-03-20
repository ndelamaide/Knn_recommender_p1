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
  // def mapNeighbors(ratings: Array[Rating]): Map[Rating, Array[Rating]] = {
  //   (u: Int) => 
  //     ratings.map(x => (ratings.filter(_.user == x.user)))
  // }

  // def jaccardSimilarityAllUsers1(ratings: Array[Rating]):  Map[(Int, Int), Double] = {
  //   /**
  //   Given a patient, med, diag, lab graph, calculate pairwise similarity between all
  //   patients. Return a RDD of (patient-1-id, patient-2-id, similarity) where 
  //   patient-1-id < patient-2-id to avoid duplications
  //   */
  //   val neighborEvents=graph.collectNeighborIds(EdgeDirection.Out)

  //   val allpatientneighbor=neighborEvents.filter(f=> f._1.toLong <= 1000)
  //   val cartesianneighbor=allpatientneighbor.cartesian(allpatientneighbor).filter(f=>f._1._1<f._2._1)
  //   cartesianneighbor.map(f=>(f._1._1,f._2._1,jaccard(f._1._2.toSet,f._2._2.toSet)))


  // }

  def mapmovies(ratings: Array[Rating]): Int => Array[Int] = { 
    (u: Int) => 
      ratings.filter(x => x.user == u).map(_.item)
  }

  def jaccard[A](a: Set[A], b: Set[A]): Double = {
    /*
    Given two sets, compute its Jaccard similarity and return its result.
    If the union part is zero, then return 0.
    */
    if (a.isEmpty || b.isEmpty){return 0.0}
    a.intersect(b).size.toFloat/a.union(b).size.toFloat

  }


  def jaccardpred12(ratings: Array[Rating]): Double = {
    val movies1 = ratings.filter(x => x.user == 1).map(_.item)
    val movies2 = ratings.filter(x => x.user == 2).map(_.item)


    jaccard(movies1.toSet, movies2.toSet)
  }

  def jaccardSimilarityAllUsers(ratings: Array[Rating]): Map[(Int, Int), Double] = {
 
    val user_set = ratings.map(x => x.user).distinct

    val user_pairs = (for(u <- user_set; v <- user_set if u < v) yield (u, v)).distinct

    val moviesmap = mapmovies(ratings)
    
    user_pairs.map(x => (x, jaccard(moviesmap(x._1).toSet, moviesmap(x._2).toSet))).toMap

  }

  
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
    * Predictor using the k-nearest neighboors
    *
    * @param ratings
    * @param k k-nearest neighboors to consider
    * @return the predictor
    */
  def predictorkNN(ratings: Array[Rating], k: Int): (Int, Int) => Double = {

    val global_avg = predictorGlobalAvg(ratings)(-1,-1)

    val users_avg = computeUsersAvg(ratings)

    val standardized_ratings = standardizeRatings(ratings, users_avg)

    val preprocessed_ratings =  preprocessRatings(standardized_ratings)

    val cosine_similarities = computeCosine(preprocessed_ratings)

    (u: Int, i: Int) =>  {
      
      users_avg.get(u) match {
        case Some(x) => {
          val ri = computeRikNN(standardized_ratings, cosine_similarities, u, i, k)
          x + ri * scale(x + ri, x)
        }
        case None => global_avg
      }
    }
  }
  
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

}