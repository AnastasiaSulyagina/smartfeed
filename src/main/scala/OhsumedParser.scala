import containers.{FeatureVector, Sample}

import scala.io.Source

/**
  * Created by anastasia.sulyagina
  */
object OhsumedParser {
  def parse(path: String): (List[Sample]) = {
    Source.fromFile(path).getLines().map(
      line => {
        val features = line.split(" ")
        (features(1), new FeatureVector(features.head.toInt,
          features.drop(2).dropRight(3).map(x => x.split(":")(1).toDouble).toList))
      }).toList.groupBy(_._1)
      .mapValues(x =>
        new Sample(x.map(_._2)))
      .values.toList
  }
  def parseTest(path: String): (List[FeatureVector]) = {
    Source.fromFile(path).getLines().map(
      line => {
        val features = line.split(" ")
        new FeatureVector(features.head.toInt,
          features.drop(2).dropRight(3).map(x => x.split(":")(1).toDouble).toList)
      }).toList
  }
}
