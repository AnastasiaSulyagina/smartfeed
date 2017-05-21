package evaluation

/**
  * Created by anastasia.sulyagina
  */
object Metrics {
  val MIN_SIZE = 30

  def AP(pred: List[Int], lab: List[Int]): Double = {
    val size = math.min(pred.size, MIN_SIZE)
    val gtPerm = lab.zipWithIndex.sortBy(x => -x._1).unzip._2.take(size)
    var score = 0.0
    for (k <- 1 to size) {
      var hits = 0.0
      if (gtPerm.take(k) contains pred(k)) {
        for ((p, i) <- pred.take(k).zipWithIndex) {
          if (gtPerm.take(k).contains(p) && !pred.take(i).contains(p)) {
            hits += 1.0
            //score += hits /// (i + 1.0)
          }
        }
      }
      score += hits / k
    }
    score / size
  }

  def MAP(aps: List[Double]): Double = aps.sum / aps.length

  def NDCG(k: Int, pred: List[Int], labels: List[Int]): Double = {
    if (pred.size >= k) {

      val predVals = pred.map(x => labels(x))
      val gt = labels.sortBy(x => -x)
      var maxDcg = 0.0
      var dcg = 0.0

      (0 until math.min(predVals.length, k)).foreach { i =>
        val gain = (Math.pow(2.0, predVals(i)) - 1.0) / math.log(i + 2)
        val maxGain = (Math.pow(2.0, gt(i)) - 1.0) / math.log(i + 2)
        dcg += gain
        maxDcg += maxGain
      }
      dcg / maxDcg
    } else {
      0.0
    }
  }
}
