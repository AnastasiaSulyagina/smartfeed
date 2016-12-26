package containers

/**
  * Created by anastasia.sulyagina
  */
class FeatureVector(var label: Double, var fvals: List[Double]) {
  def fcount = fvals.length
}
