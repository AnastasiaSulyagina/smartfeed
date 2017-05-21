package parsing
import java.io.{File, FileWriter}

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

  def toLetorFormat(data: List[(List[Sample], List[FeatureVector])]) = {
    val path = "/Users/anastasia/Development/smartfeed/data/"

    val fwTrain = new FileWriter(path + "formatted_train", true)
    val fwVal = new FileWriter(path + "formatted_val", true)
    val fwTest = new FileWriter(path + "formatted_test", true)

    for (i <- data.indices) {
      for (train <- data(i)._1.head.data.dropRight(data(i)._1.head.data.size / 5)) {
        fwTrain.write(train.label + " qid:" + i + " " + train.fvals.zip(Stream from 1)
          .map{case(x, ind) => ind.toString + ":" + x.toString}.mkString(" ") + "\n")
      }
      for (train <- data(i)._1.head.data.drop(data(i)._1.head.data.size / 5 * 4)) {
        fwVal.write(train.label + " qid:" + i + " " + train.fvals.zip(Stream from 1)
          .map{case(x, ind) => ind.toString + ":" + x.toString}.mkString(" ") + "\n")
      }
      for (test <- data(i)._2) {
        fwTest.write(test.label + " qid:" + i + " " + test.fvals.zip(Stream from 1)
          .map{case(x, ind) => ind.toString + ":" + x.toString}.mkString(" ") + "\n")
      }
    }
    fwTrain.close()
    fwVal.close()
    fwTest.close()
  }
}
