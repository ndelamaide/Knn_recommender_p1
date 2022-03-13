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

  val measurements = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    Thread.sleep(1000) // Do everything here from train and test
    42        // Output answer as last value
  }))
  val timings = measurements.map(t => t._2) // Retrieve the timing measurements

  // Save answers as JSON
  def printToFile(content: String, 
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }


  var GlobalAvg= globalavg(train)

  

  val B11 = GlobalAvg
  val B12 = predictor_user_avg(train)(1, -1)
  val B13 = predictor_item_avg(train)(-1, 1)

  val train_normalized = normalizeddev_all(train)
  val B14 = dev_avg_item_i(train_normalized, 1)
  val B15 = predictor_useru_itemi(train)(1, 1)

  val t20 = System.nanoTime()
  val B21 = Global_Avg_MAE(test, train)
  val t21 = System.nanoTime()
  val B22 = User_Avg_MAE(test, train)
  val t22 = System.nanoTime()
  val B23 = Item_Avg_MAE(test, train)
  val t23 = System.nanoTime()
  val B24 = Baseline_Avg_MAE(test, train)
  val t24 = System.nanoTime()



  val B31 = 0
  val B32 = 0
  val B33 = 0
  val B34 = 0

  val NanotoMs = Math.pow(10,-6)

  
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
            "average (ms)" -> ujson.Num((t21-t20)*NanotoMs), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
          ),
          "2.UserAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num((t22-t21)*NanotoMs), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
          ),
          "3.ItemAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num((t23-t22)*NanotoMs), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
          ),
          "4.Baseline" -> ujson.Obj(
            "average (ms)" -> ujson.Num((t24-t23)*NanotoMs), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
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
