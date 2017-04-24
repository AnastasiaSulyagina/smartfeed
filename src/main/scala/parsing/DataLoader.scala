package parsing
import java.io.File

import breeze.linalg._
import containers.{FeatureVector, Sample}

import scala.io.Source

/**
  * Created by anastasia.sulyagina
  */
object DataLoader {
  val dataDir = "/Users/anastasia/Development/smartfeed/data/grouped/"
  /*def Load(path: String): DenseMatrix[Double] = {
    val data: DenseMatrix[Double] = csvread(new File("part0"))
    data
  }*/
  def loadTrain(user: String) =
    List(new Sample(Source.fromFile(dataDir + user + "/train.csv").getLines()
      .map(l => {
      val features = l.split(",").map(_.toDouble).toList
      new FeatureVector(features.last, features.drop(2).dropRight(1))
    }).toList))

  def loadTest(user: String) =
    Source.fromFile(dataDir + user + "/test.csv").getLines()
      .map(l => {
        val features = l.split(",").map(_.toDouble).toList
        new FeatureVector(features.last, features.drop(2).dropRight(1))
      }).toList
}
