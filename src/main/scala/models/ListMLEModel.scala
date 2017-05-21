package models

import containers.{FeatureVector, Sample}
import gnu.trove.list.array.TDoubleArrayList

import scala.collection.mutable
import scala.math._

/**
  * Created by anastasia.sulyagina
  */
class ListMLEModel extends RankingModel{
  override var parameters: List[Double] = _
  override val learningRate = 0.0002
  override val modelName = "ListMLE"
  val toleranceRate = 0.03

  override def trainModel(samples: List[Sample]): Unit = {
    parameters = List.fill(samples.head.data.head.fcount)(0.0)
    var loss: Double = 0.0
    var toStop = false
    var it = 0
    while (!toStop && it < 100) {
      it += 1
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
    println()
  }

  def maxIndex(l: List[Double]): Int =
    l.zipWithIndex.maxBy(_._1)._2

  def calculateGradient(fvvAll: List[FeatureVector], newParams: List[Double]): Array[Double] = {
    val fvv = fvvAll.filter(x => scala.util.Random.nextInt(100) > 40)
    val maxScoreIndex = maxIndex(fvv.map(x => x.label))
    val expsumZ = fvv.foldLeft(0.0) ((acc, fv) => acc + exp(calculateInnerProduct(newParams, fv.fvals)))

    val probZ = fvv.map(fv => exp(calculateInnerProduct(newParams, fv.fvals)))
    val grad = new TDoubleArrayList(fvv.head.fcount)
    grad.fill(0, fvv.head.fcount, 0.0)

    for(i <- 0 until grad.size()) {
      for (j <- fvv.indices) {
        grad.setQuick(i, fvv(j).fvals(i) * probZ(j))
      }
      grad.setQuick(i, grad.getQuick(i) / expsumZ - fvv(maxScoreIndex).fvals(i))
    }
    grad.toArray
  }
}
