package containers

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by anastasia.sulyagina
  */
class FeatureVector(var label: Double, var fvals: List[Double]) {
  def fcount = fvals.length
  def asLabeledPoint() = LabeledPoint(label, Vectors.dense(fvals.toArray))

  def asLabeledPointFrom1() = LabeledPoint((label + 1).toInt, Vectors.dense(fvals.toArray))
}
