package distributed

import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import ujson._

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math
import shared.predictions._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default=Some("\t"))
  val master = opt[String](default=Some(""))
  val num_measurements = opt[Int](default=Some(0))
  val json = opt[String]()
  verify()
}

object DistributedBaseline extends App {
  var conf = new Conf(args) 

  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = if (conf.master() != "") {
    SparkSession.builder().master(conf.master()).getOrCreate()
  } else {
    SparkSession.builder().getOrCreate()
  }
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  println("Loading training data from: " + conf.train()) 
  val train = load(spark, conf.train(), conf.separator())
  println("Loading test data from: " + conf.test()) 
  val test = load(spark, conf.test(), conf.separator())

  val measurements = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    val predictor_rating = predictorRating(train)
    MAE(test, predictor_rating)
  }))

  val timings = measurements.map(t => t._2) // Retrieve the timing measurements
  
  val predictor_global_avg = predictorGlobalAvg(train)
  val predictor_user_avg = predictorUserAvg(train)
  val predictor_item_avg = predictorItemAvg(train)
  val predictor_rating = predictorRating(train)

  val D11 = predictor_global_avg(1, 1) // Global average
  val D12 = predictor_user_avg(1, 1) // User 1 average
  val D13 = predictor_item_avg(1, 1) // Item 1 average

  //val users_avg = computeUsersAvg(train)
  val D14 = 0.0//computeItemsGlobalDev(train, users_avg)(1)

  val D15 = predictor_rating(1, 1) // Pred rating for user 1 item 1
  val D16 = MAE(test, predictor_rating) //MAE

  // Save answers as JSON
  def printToFile(content: String, 
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ; 
    case Some(jsonFile) => {
      val answers = ujson.Obj(
        "Meta" -> ujson.Obj(
          "1.Train" -> conf.train(),
          "2.Test" -> conf.test(),
          "3.Master" -> conf.master(),
          "4.Measurements" -> conf.num_measurements()
        ),
        "D.1" -> ujson.Obj(
          "1.GlobalAvg" -> ujson.Num(D11), // Datatype of answer: Double
          "2.User1Avg" -> ujson.Num(D12),  // Datatype of answer: Double
          "3.Item1Avg" -> ujson.Num(D13),   // Datatype of answer: Double
          "4.Item1AvgDev" -> ujson.Num(D14), // Datatype of answer: Double,
          "5.PredUser1Item1" -> ujson.Num(D15), // Datatype of answer: Double
          "6.Mae" -> ujson.Num(D16) // Datatype of answer: Double
        ),
        "D.2" -> ujson.Obj(
          "1.DistributedBaseline" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
          )            
        )
      )
      val json = write(answers, 4)

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
