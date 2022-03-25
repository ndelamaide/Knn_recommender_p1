package test.predict

import org.scalatest._
import funsuite._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import shared.predictions._
import tests.shared.helpers._
import ujson._

class PersonalizedTests extends AnyFunSuite with BeforeAndAfterAll {

   val separator = "\t"
   var spark : org.apache.spark.sql.SparkSession = _

   val train2Path = "data/ml-100k/u2.base"
   val test2Path = "data/ml-100k/u2.test"
   var train2 : Array[shared.predictions.Rating] = null
   var test2 : Array[shared.predictions.Rating] = null

   override def beforeAll {
       Logger.getLogger("org").setLevel(Level.OFF)
       Logger.getLogger("akka").setLevel(Level.OFF)
       spark = SparkSession.builder()
           .master("local[1]")
           .getOrCreate()
       spark.sparkContext.setLogLevel("ERROR")
       // For these questions, train and test are collected in a scala Array
       // to not depend on Spark
       train2 = load(spark, train2Path, separator).collect()
       test2 = load(spark, test2Path, separator).collect()

       //Pre-compute global variables
      global_avg = computeGlobalAvg(train2)
      users_avg = computeUsersAvg(train2)
      standardized_ratings = standardizeRatings(train2, users_avg)
      preprocessed_ratings = preprocessRatings(standardized_ratings)
      preprocessed_groupby_user = preprocessed_ratings.groupBy(_.user)
      standardized_groupby_item = standardized_ratings.groupBy(_.item)
      similarities_uniform = computeSimilaritiesUniform(train2)
      similarities_cosine = computeCosine(preprocessed_ratings)
   }

   // All the functions definitions for the tests below (and the tests in other suites) 
   // should be in a single library, 'src/main/scala/shared/predictions.scala'.

   // Provide tests to show how to call your code to do the following tasks.
   // Ensure you use the same function calls to produce the JSON outputs in
   // src/main/scala/predict/Baseline.scala.
   // Add assertions with the answer you expect from your code, up to the 4th
   // decimal after the (floating) point, on data/ml-100k/u2.base (as loaded above).

   test("Test uniform unary similarities") { 

    // Global variables already computed above

     // Create predictor with uniform similarities
     val predictor_uniform = predictorUniform(train2)
     
     // Compute personalized prediction for user 1 on item 1
     assert(within(predictor_uniform(1, 1), 4.0468, 0.0001))

     // MAE 
     assert(within(MAE(test2, predictor_uniform), 0.7604, 0.0001))
   } 

   test("Test ajusted cosine similarity") { 

    // Global variables already computed above

     // Create predictor with adjusted cosine similarities
     val predictor_cosine = predictorCosine(train2)

     // Similarity between user 1 and user 2
     assert(within(adjustedCosine(preprocessed_groupby_user, 1, 2),  0.0730, 0.0001))

     // Compute personalized prediction for user 1 on item 1
     assert(within(predictor_cosine(1, 1), 4.0870, 0.0001))

     // MAE 
     assert(within(MAE(test2, predictor_cosine), 0.7372, 0.0001))
   }

   test("Test jaccard similarity") { 
     // Create predictor with jaccard similarities
     val pred_jaccard = predictorJaccard(train2)

     // Similarity between user 1 and user 2
     assert(within(jaccardpred12(train2), 0.0318, 0.0001))

     // Compute personalized prediction for user 1 on item 1
     assert(within(pred_jaccard(1, 1), 4.0982, 0.0001))

     // MAE 
     assert(within(MAE(test2, pred_jaccard), 0.7556, 0.0001))
   }
}
