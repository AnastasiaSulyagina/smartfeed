package models

import java.io.{BufferedWriter, FileWriter}

import containers.{FeatureVector, Sample}
import evaluation.Metrics

import scala.collection.mutable

/**
  * Created by anastasia.sulyagina
  */
trait RankingModel {
  var parameters: List[Double]
  val learningRate: Double
  val modelName: String

  def trainModel(trainData: List[Sample]): Unit

  protected def calculateInnerProduct(parameters: List[Double], features: List[Double]): Double =
    parameters.zip(features).map{case (p: Double, f: Double) => p * f}.sum

  protected def updateParameters(newParams: List[Double], gradient: Array[Double]): (List[Double]) =
    newParams.zip(gradient).map {
      case (value: Double, grad: Double) => value - grad * learningRate
    }

  def rank(fvv: List[FeatureVector], path: String = "/Users/anastasia/Development/smartfeed/data/"): (List[(Double, Double)]) = {
    //print("In progress: " + path.split("/").last)
    //val fw = new FileWriter(path + modelName + "_result.txt", true)

    val scores = fvv.map(fv => {
      var score = 0.0
      for (j <- fv.fvals.indices) {
        score += fv.fvals(j) * parameters(j)
      }
      score
    })
    val labels = fvv.map(_.label.toInt)
    //for (score <- scores) {fw.write(score.toString + "\n")}

    //fw.close()
    scores.zip(labels.map(_.toDouble + 1.0))
  }
}
