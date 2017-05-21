package models
import containers.{FeatureVector, Sample}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
  * Created by anastasia.sulyagina
  */
class RankSVM extends RankingModel {
  override var parameters: List[Double] = _
  override val learningRate: Double = 0.01
  override val modelName: String = "RankSVM"
  val numIterations = 50

  val spark = SparkSession
    .builder()
    .appName("SmartFeed")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext
  var model: SVMModel = _

  sc.setLogLevel("ERROR")
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  private val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

  def combine(list1: List[FeatureVector], list2: List[FeatureVector]) =
    for(x <- list1; y <- list2; r = Random.nextInt(1))
      yield new FeatureVector(
        if (r == 0) 1 else 0, //x.label - y.label else y.label - x.label,
        x.fvals.zip(y.fvals).map{case (a, b) => if (r == 0) a - b else b - a})

  // Extracts all pairs of posts with different reactions for each user and concatenates
  def transformToPairs(trainData: List[Sample]): List[FeatureVector] =
    trainData.flatMap(x => {
      val (good, ignored, bad) = (x.data.filter(_.label == 1), x.data.filter(_.label == 0), x.data.filter(_.label == -1))
      if (good.size < 3 && bad.size < 3 && ignored.size < 3)
        List.empty
      else {
        combine(Random.shuffle(good).take(3), Random.shuffle(ignored).take(3)) ++
          combine(Random.shuffle(good).take(3), Random.shuffle(bad).take(3)) ++
          combine(Random.shuffle(ignored).take(3), Random.shuffle(bad).take(3))
      }
    })

  override def trainModel(trainData: List[Sample]): Unit = {
    val pairData = transformToPairs(trainData)
    model = SVMWithSGD.train(sc.parallelize(
      pairData.map(x => LabeledPoint(x.label, Vectors.dense(x.fvals.toArray)))),
      numIterations)
  }

    override def rank(fvv: List[FeatureVector], path: String): (List[(Double, Double)]) = {
      val (scores, labels) = fvv.map(fv => {
        val prediction = fv.fvals.zip(model.weights.toArray).map(x => x._1 * x._2).sum
        (prediction, fv.label.toInt + 1)
      }).unzip

      //val scorePerm = scores.zipWithIndex.sortBy(x => x._1 * -1).unzip._2
      scores.zip(labels.map(_.toDouble))
  }
}
