package models

import containers.{FeatureVector, Sample}
import scala.collection.mutable
import scala.math._

/**
  * Created by anastasia.sulyagina
  */

class ListNetModel extends RankingModel {
  var parameters: List[Double] = _
  override val learningRate = 0.001
  override val modelName = "ListNet"
  val iterations = 15

  override def trainModel(samples: List[Sample]): Unit = {
    parameters = List.fill(samples.head.data.head.fcount)(0.0)

    for (iter_num <- 1 to iterations){
      for (sample <- samples) {
        val gradient = calculateGradient(sample.data, parameters)
        parameters = updateParameters(parameters, gradient)
      }
    }
  }

  def calculateGradient(fvv: List[FeatureVector], newParams: List[Double]): List[Double] = {
    val expsumY = fvv.map(fv => exp(fv.label)).sum
    val expsumZ = fvv.map(fv => exp(calculateInnerProduct(newParams, fv.fvals))).sum

    val probY = fvv.map(fv => exp(fv.label) / expsumY)
    val probZ = fvv.map(fv => exp(calculateInnerProduct(newParams, fv.fvals)) / expsumZ)
    val grad = mutable.MutableList.fill(fvv.head.fcount)(0.0)

    for (i <- fvv.indices) {
      fvv(i).fvals.zipWithIndex.foreach{
        case (x, ind) =>
          grad(ind) += probZ(i) * x - probY(i) * x
      }}
    grad.toList
  }
}
