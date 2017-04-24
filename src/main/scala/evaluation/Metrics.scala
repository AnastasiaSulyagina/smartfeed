package evaluation

/**
  * Created by anastasia.sulyagina
  */
object Metrics {
  val MIN_SIZE = 30

  def AP(pred: List[Int], lab: List[Int]): Double = {
    val gtPerm = lab.zipWithIndex.sortBy(x => -x._1).unzip._2.take(MIN_SIZE)
    var score = 0.0
    var hits = 0.0

    for ((p, i) <- pred.take(MIN_SIZE).zipWithIndex) {
      if (gtPerm.contains(p) && !pred.take(i).contains(p)) {
        hits += 1.0
        score += hits / (i + 1.0)
      }
    }
    score / MIN_SIZE
  }

  def MAP(aps: List[Double]): Double = aps.sum / aps.length

  def NDCG(k: Int, pred: List[Int], lab: List[Int]): Double = {
    val labSet = lab.toSet
    val predVals = pred.map(x => lab(x))

    val gt = lab.sortBy(x => -x)
    var maxDcg = 0.0
    var dcg = 0.0

    (0 to math.min(math.max(predVals.length, labSet.size), k)).foreach { i =>
      val gain = Math.pow(2.0, predVals(i)) - 1.0 / math.log(i + 2)
      val maxGain = Math.pow(2.0, gt(i)) - 1.0 / math.log(i + 2)
      dcg += gain
      maxDcg += maxGain
    }
    dcg / maxDcg
  }
}
