package models

import containers.{FeatureVector, Sample}

import scala.collection.mutable
import scala.math._

/**
  * Created by anastasia.sulyagina
  */
class ListMLEModel extends RankingModel{
  override var parameters: List[Double] = _
  override val learningRate = 0.0001
  override val modelName = "ListMLE"
  val toleranceRate = 0.02

  override def trainModel(samples: List[Sample]): Unit = {
    parameters = List.fill(samples.head.data.head.fcount)(0.0)
    var loss: Double = 0.0
    var toStop = false

    while (!toStop){
      var curLoss: Double = 0.0
      for (sample <- samples) {
        val gradient = calculateGradient(sample.data, parameters)
        parameters = updateParameters(parameters, gradient)
      }
      for (sample <- samples) {
        //update loss
        curLoss += log(sample.data.foldLeft(0.0)((acc, fv) =>
          acc + exp(calculateInnerProduct(parameters, fv.fvals))))
        curLoss -= calculateInnerProduct(parameters,
          sample.data(maxIndex(sample.data.map(x => x.label))).fvals)
      }
      if (abs(loss - curLoss) < toleranceRate) {
        toStop = true
      } else {
        loss = curLoss
      }
    }
  }

  def maxIndex(l: List[Double]): Int =
    l.zipWithIndex.maxBy(_._1)._2

  def calculateGradient(fvv: List[FeatureVector], newParams: List[Double]): List[Double] = {
    val maxScoreIndex = maxIndex(fvv.map(x => x.label))
    val expsumZ = fvv.foldLeft(0.0) ((acc, fv) => acc + exp(calculateInnerProduct(newParams, fv.fvals)))

    val probZ = fvv.map(fv => exp(calculateInnerProduct(newParams, fv.fvals)))
    val grad = mutable.MutableList.fill(fvv.head.fcount)(0.0)

    for(i <- grad.indices) {
      for (j <- fvv.indices) {
        grad(i) += fvv(j).fvals(i) * probZ(j)
      }
      grad(i) = grad(i) / expsumZ - fvv(maxScoreIndex).fvals(i)
    }
    grad.toList
  }
}
