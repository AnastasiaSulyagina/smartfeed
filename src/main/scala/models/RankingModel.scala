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

  def calculateInnerProduct(parameters: List[Double], features: List[Double]): Double =
    parameters.zip(features).map{case (p: Double, f: Double) => p * f}.sum

  def updateParameters(newParams: List[Double], gradient: List[Double]): List[Double] =
    newParams.zip(gradient).map{
      case (value: Double, grad: Double) => value - grad * learningRate
    }

  private def getMetrics(scorePerm: List[Int], labels: List[Int]) =
    List(5, 10, 30, 50).map(x => (x, Metrics.NDCG(x, scorePerm, labels))) :+ (-1, Metrics.AP(scorePerm, labels))

  def rank(path: String, fvv: List[FeatureVector]): List[Double] = {
    //print("In progress: " + path.split("/").last)
    //val fw = new FileWriter(path + "/" + modelName + "_result.txt")

    val scores = fvv.map(fv => {
      var score = 0.0
      for (j <- fv.fvals.indices) {
        score += fv.fvals(j) * parameters(j)
      }
      score
    })
    val labels = fvv.map(_.label.toInt)
    val scorePerm = scores.zipWithIndex.sortBy(x => -x._1).unzip._2
    //scores.foreach(score => fw.write(score.toString + "\n"))

    //fw.close()
    val metrics = getMetrics(scorePerm, labels)
    metrics.foreach{case (n, x) => if (n == -1) print(f"  AP: $x%1.4f") else print(f"  NDCG@$n: $x%1.4f")}
    print(s"   size: ${scores.size}")
    println()
    metrics.map(_._2)
  }
}
