package recommend

import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import ujson._

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import shared.predictions._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val personal = opt[String](required = true)
  val separator = opt[String](default = Some("\t"))
  val json = opt[String]()
  verify()
}

object Recommender extends App {
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
  println("Loading data from: " + conf.data()) 
  val data = load(spark, conf.data(), conf.separator()).collect()
  assert(data.length == 100000, "Invalid data")

  println("Loading personal data from: " + conf.personal()) 
  val personalFile = spark.sparkContext.textFile(conf.personal())
  val personal = personalFile.map(l => {
      val cols = l.split(",").map(_.trim)
      if (cols(0) == "id") 
        Rating(944,0,0.0)
      else 
        if (cols.length < 3) 
          Rating(944, cols(0).toInt, 0.0)
        else
          Rating(944, cols(0).toInt, cols(2).toDouble)
  }).filter(r => r.rating != 0).collect()
  val movieNames = personalFile.map(l => {
      val cols = l.split(",").map(_.trim)
      if (cols(0) == "id") (0, "header")
      else (cols(0).toInt, cols(1).toString)
  }).collect().toMap

  val train = data ++ personal
  
  global_avg = computeGlobalAvg(train)
  users_avg = computeUsersAvg(train)
  standardized_ratings = standardizeRatings(train, users_avg)
  preprocessed_ratings = preprocessRatings(standardized_ratings)
  preprocessed_groupby_user = preprocessed_ratings.groupBy(_.user)
  standardized_groupby_item = standardized_ratings.groupBy(_.item)
  similarities_cosine = computeCosine(preprocessed_ratings)
  user_similarities = computeUserSimilarities(train)
  
  val predictor_allNN = predictorAllNN(train)
  val predictor_300NN = predictor_allNN(300)

  val R1 = predictor_300NN(1, 1)

  val R2 = recommendMovies(train, predictor_300NN, 944, 3)


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
          "data" -> conf.data(),
          "personal" -> conf.personal()
        ),
        "R.1" -> ujson.Obj(
          "PredUser1Item1" -> R1 // Prediction for user 1 of item 1
        ),
          // IMPORTANT: To break ties and ensure reproducibility of results,
          // please report the top-3 recommendations that have the smallest
          // movie identifier.

        "R.2" -> List(R2.apply(0), R2.apply(1), R2.apply(2)).map(x => ujson.Arr(x._1, movieNames(x._1), x._2))
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
