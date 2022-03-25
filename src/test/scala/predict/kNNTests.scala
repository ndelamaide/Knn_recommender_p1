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

class kNNTests extends AnyFunSuite with BeforeAndAfterAll {

   val separator = "\t"
   var spark : org.apache.spark.sql.SparkSession = _

   val train2Path = "data/ml-100k/u2.base"
   val test2Path = "data/ml-100k/u2.test"
   var train2 : Array[shared.predictions.Rating] = null
   var test2 : Array[shared.predictions.Rating] = null

  // var adjustedCosine_ : Map[Int, Map[Int, Double]] = null

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
   }


   val predictor_allNN = predictorAllNN(train2)

   // All the functions definitions for the tests below (and the tests in other suites) 
   // should be in a single library, 'src/main/scala/shared/predictions.scala'.

   // Provide tests to show how to call your code to do the following tasks.
   // Ensure you use the same function calls to produce the JSON outputs in
   // src/main/scala/predict/Baseline.scala.
   // Add assertions with the answer you expect from your code, up to the 4th
   // decimal after the (floating) point, on data/ml-100k/u2.base (as loaded above).
   test("kNN predictor with k=10") { 
     // Create predictor on train2
    val predictor_10NN = predictor_allNN(10)

    // Necessary to compute similarities
    val users_avg = computeUsersAvg(train2)
    val standardized_ratings = standardizeRatings(train2, users_avg)
    val preprocessed_ratings =  preprocessRatings(standardized_ratings)

     //Similarity between user 1 and itself
    assert(within(adjustedCosine(preprocessed_ratings, 1, 1), 0.0, 0.0001))
 
     // Similarity between user 1 and 864
     assert(within(adjustedCosine(preprocessed_ratings, 1, 864), 0.2423, 0.0001))

     // Similarity between user 1 and 886
     assert(within(adjustedCosine(preprocessed_ratings, 1, 886), 0.2140, 0.0001))

     // Prediction user 1 and item 1
     assert(within(predictor_10NN(1, 1), 4.3622, 0.0001))

     // MAE on test2 
     assert(within(MAE(test2, predictor_10NN), 0.7451, 0.0001))
   } 

   test("kNN Mae") {
     // Compute MAE for k around the baseline MAE
    
    val predictor_52NN = predictorAllNN(train2)(52)
    val MAEKNN52 = MAE(test2, predictor_52NN)
    assert(within(MAEKNN52, 0.7610, 0.0001))

    val predictor_53NN = predictorAllNN(train2)(53)
    val MAEKNN53 = MAE(test2, predictor_53NN)
    assert(within(MAEKNN53, 0.7601, 0.0001))

     // Ensure the MAEs are indeed lower/higher than baseline
    val baselineMAE = 0.7604

    assert(MAEKNN52>baselineMAE)

    assert(MAEKNN53<baselineMAE)
   }
}
