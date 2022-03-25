package predict

import org.rogach.scallop._
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math
import shared.predictions._
import org.spark_project.jetty.server.Authentication.User


class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default=Some("\t"))
  val num_measurements = opt[Int](default=Some(0))
  val json = opt[String]()
  verify()
}

object Baseline extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  var conf = new Conf(args) 
  // For these questions, data is collected in a scala Array 
  // to not depend on Spark
  println("Loading training data from: " + conf.train()) 
  val train = load(spark, conf.train(), conf.separator()).collect()
  println("Loading test data from: " + conf.test()) 
  val test = load(spark, conf.test(), conf.separator()).collect()

  val MeasurementsGlobalAvgMae = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    val predGA = predictorGlobalAvg(train)
    MAE(test, predGA)
  }))
  val MeasurementsUserAvgMae = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    val predUA = predictorUserAvg(train)
    MAE(test, predUA)
  }))
  val MeasurementsItemAvgMae= (1 to conf.num_measurements()).map(x => timingInMs(() => {
    val predIA = predictorItemAvg(train)
    MAE(test, predIA)
  }))
  val MeasurementsBaselineMae = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    val predBa = predictorRating(train)
    MAE(test, predBa)
  }))

  val timingsGlobalAvgMae = MeasurementsGlobalAvgMae.map(t => t._2) // Retrieve the timing measurements
  val timingsUserAvgMae = MeasurementsUserAvgMae.map(t => t._2) // Retrieve the timing measurements
  val timingsItemAvgMae = MeasurementsItemAvgMae.map(t => t._2) // Retrieve the timing measurements
  val timingsBaselineMae = MeasurementsBaselineMae.map(t => t._2) // Retrieve the timing measurements



  // Save answers as JSON
  def printToFile(content: String, 
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }

  val global_avg = predictorGlobalAvg(train)(-1, -1)

  val B11 = global_avg
  val B12 = predictorUserAvg(train)(1, -1)
  val B13 = predictorItemAvg(train)(-1, 1)

  val users_avg = computeUsersAvg(train)

  val standardized_ratings = standardizeRatings(train, users_avg)
  val items_global_dev = computeItemsGlobalDev(train, users_avg)

  val B14 = items_global_dev.getOrElse(1, 0.0)
  val B15 = predictorRating(train)(1, 1)


  val predGA = predictorGlobalAvg(train) 
  val B21 = MAE(test, predGA)

  val predUA = predictorUserAvg(train)
  val B22 = MAE(test, predUA)

  val predIA = predictorItemAvg(train)
  val B23 = MAE(test, predIA)

  val predBa = predictorRating(train)
  val B24 = MAE(test, predBa)
  
  conf.json.toOption match {
    case None => ; 
    case Some(jsonFile) => {
      var answers = ujson.Obj(
        "Meta" -> ujson.Obj(
          "1.Train" -> ujson.Str(conf.train()),
          "2.Test" -> ujson.Str(conf.test()),
          "3.Measurements" -> ujson.Num(conf.num_measurements())
        ),
        "B.1" -> ujson.Obj(
          "1.GlobalAvg" -> ujson.Num(B11), // Datatype of answer: Double
          "2.User1Avg" -> ujson.Num(B12),  // Datatype of answer: Double
          "3.Item1Avg" -> ujson.Num(B13),   // Datatype of answer: Double
          "4.Item1AvgDev" -> ujson.Num(B14), // Datatype of answer: Double
          "5.PredUser1Item1" -> ujson.Num(B15) // Datatype of answer: Double
        ),
        "B.2" -> ujson.Obj(
          "1.GlobalAvgMAE" -> ujson.Num(B21), // Datatype of answer: Double
          "2.UserAvgMAE" -> ujson.Num(B22),  // Datatype of answer: Double
          "3.ItemAvgMAE" -> ujson.Num(B23),   // Datatype of answer: Double
          "4.BaselineMAE" -> ujson.Num(B24)   // Datatype of answer: Double
        ),
        "B.3" -> ujson.Obj(
          "1.GlobalAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timingsGlobalAvgMae)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timingsGlobalAvgMae)) // Datatype of answer: Double
          ),
          "2.UserAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timingsUserAvgMae)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timingsUserAvgMae)) // Datatype of answer: Double
          ),
          "3.ItemAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timingsItemAvgMae)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timingsItemAvgMae)) // Datatype of answer: Double
          ),
          "4.Baseline" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timingsBaselineMae)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timingsBaselineMae)) // Datatype of answer: Double
          )
        )
      )
      val json = ujson.write(answers, 4)
      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json.toString, jsonFile)
    }
  }

  println("")
  spark.close()
}
