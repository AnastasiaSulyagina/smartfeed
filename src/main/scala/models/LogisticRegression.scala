package models
import containers.{FeatureVector, Sample}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Created by anastasia.sulyagina
  */
class LogisticRegression extends RankingModel{
  override var parameters: List[Double] = _
  var model: LogisticRegressionModel = _
  override val learningRate: Double = 0.01
  override val modelName: String = "LogisticRegression"


  val spark = SparkSession
    .builder()
    .appName("SmartFeed")
    .config("spark.master", "local")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  private val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  override def trainModel(trainData: List[Sample]): Unit = {
    model = new LogisticRegressionWithLBFGS()
      .setNumClasses(3)
      .run(sc.parallelize(trainData.flatMap(sample =>
        sample.data.map(x =>
          x.asLabeledPointFrom1()))))
  }

  override def rank(fvv: List[FeatureVector], path: String): (List[(Double, Double)]) = {
    val (scores, labels) = fvv.map(fv => {
      val prediction = model.predict(Vectors.dense(fv.fvals.toArray))
      (prediction, fv.label.toInt + 1)
    }).unzip

    //val scorePerm = scores.zipWithIndex.sortBy(x => -x._1).unzip._2
    scores.zip(labels.map(_.toDouble))
  }
}
