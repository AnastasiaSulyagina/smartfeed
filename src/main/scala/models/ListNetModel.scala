package models

import containers.{FeatureVector, Sample}

import scala.collection.mutable
import scala.math._
import gnu.trove.list.array.{TDoubleArrayList, TLongArrayList}

/**
  * Created by anastasia.sulyagina
  */

class ListNetModel extends RankingModel {
  var parameters: List[Double] = _
  override val learningRate = 0.001
  override val modelName = "ListNet"
  val iterations = 200

  override def trainModel(samples: List[Sample]): Unit = {
    parameters = List.fill(samples.head.data.head.fcount)(0.0)

    for (iter_num <- 1 to iterations){
      for (sample <- samples) {
        parameters = updateParameters(parameters, calculateGradient(sample.data))
      }
    }
  }

  def calculateGradient(fvvAll: List[FeatureVector]): Array[Double] = {
    val fvv = fvvAll.filter(x => scala.util.Random.nextInt(100) > 40)
    val expsumY = fvv.map(fv => exp(fv.label)).sum
    val expsumZ = fvv.map(fv => exp(calculateInnerProduct(parameters, fv.fvals))).sum

    val probY = fvv.map(fv => exp(fv.label) / expsumY)
    val probZ = fvv.map(fv => exp(calculateInnerProduct(parameters, fv.fvals)) / expsumZ)

    val grad = new TDoubleArrayList(fvv.head.fcount)
    grad.fill(0, fvv.head.fcount, 0.0)

    for (i <- fvv.indices) {
      fvv(i).fvals.zipWithIndex.foreach {
        case (x, ind) => grad.setQuick(ind, grad.getQuick(ind) + probZ(i) * x - probY(i) * x)
      }}
    grad.toArray
  }
}
