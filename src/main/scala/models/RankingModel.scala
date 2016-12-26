package models

import java.io.{BufferedWriter, FileWriter}

import containers.{FeatureVector, Sample}

/**
  * Created by anastasia.sulyagina
  */
trait RankingModel {
  var parameters: List[Double]
  val learningRate: Double
  val modelName: String

  def trainModel(trainData: List[Sample]): Unit

  def calculateInnerProduct(parameters: List[Double], features: List[Double]): Double =
    parameters.zip(features).map{case (p: Double, f: Double) => p * f}.sum

  def updateParameters(newParams: List[Double], gradient: List[Double]): List[Double] =
    newParams.zip(gradient).map{
      case (value: Double, grad: Double) => value - grad * learningRate
    }

  def rank(fvv: List[FeatureVector]): Unit = {
    println("Ranking in progress")
    val bw = new BufferedWriter(new FileWriter(modelName + "_result.txt"))

    fvv.foreach(fv => {
      var score = 0.0
      for (j <- fv.fvals.indices) {
        score += fv.fvals(j) * parameters(j)
      }
      bw.write(score.toString + "\n")
    })
    bw.close()
    //testFvv.sortBy(_.label)
  }
}
