package evaluation

import java.io.FileWriter

import containers.{FeatureVector, Sample}
import models._
import parsing.{DataLoader, FileSystemHelper, OhsumedParser}

import scala.collection.mutable
import scala.io.Source

/**
  * Created by anastasia.sulyagina
  */
object Evaluation {
  val path = "/Users/anastasia/Development/smartfeed/data/"

  def getCommonResults(model: RankingModel, data: List[(List[Sample], List[FeatureVector])]) = {
    val (train, test) = (data.flatMap(x => x._1), data.map(x => x._2))

    println("Training common " + model.modelName + " model")
    model.trainModel(train)

    val fwCommon = new FileWriter(path + model.modelName + "_common.txt", true)
    val fwres = new FileWriter(path + model.modelName + "_common_r.txt", true)

    val resultCommon = test.zipWithIndex.map{case (fv, i) => {
      val res = model.rank(fv)
      val scorePerm = res.map(_._1).zipWithIndex.sortBy(x => -x._1).unzip._2
      val metricsIndexed = getMetrics(scorePerm, res.map(_._2.toInt))
      val metrics = metricsIndexed.map(_._2)
      res.foreach{x => fwCommon.write(i.toString + "\t" + x._1 + "\t" + x._2 + "\n")}
      fwres.write(metrics.map(x => f"$x%1.4f").mkString("\t") + "\t" + scorePerm.size.toString + "\n")
      (metrics(0), metrics(1), metrics(2), metrics(3), metrics(4), scorePerm.size)
    }}
    fwCommon.close()
    fwres.close()
    resultCommon
  }

  def getPersonalResults(data: List[(List[Sample], List[FeatureVector])], personalName: String = "ListNet") = {
    println("Training personal " + personalName + " model")
    val sbRes: StringBuilder = new StringBuilder
    val sbPers: StringBuilder = new StringBuilder

    val resultPersonal = data.zipWithIndex.map {
      case ((train, test), i) =>
        print(".")
        val res = runModel(getModelByName(personalName), train, test)
        val scorePerm = res.map(_._1).zipWithIndex.sortBy(x => -x._1).unzip._2
        val metrics = getMetrics(scorePerm, res.map(_._2.toInt)).map(_._2)
        res.foreach{x => sbPers.append(i.toString + "\t" + x._1 + "\t" + x._2 + "\n")}

        sbRes.append(metrics.map(x => f"$x%1.4f").mkString("\t") + "\t" + scorePerm.size.toString + "\t" + train.head.data.size.toString + "\n")
        //fwPersonal.write(metrics.map(x => f"$x%1.4f").mkString("\t") + "\t" + res._1.size.toString + "\t" + train.head.data.size.toString + "\n")
        (metrics(0), metrics(1), metrics(2), metrics(3), metrics(4), scorePerm.size, train.head.data.size)
    }
    //println()
    val fwPersonal = new FileWriter(path + personalName + "_personal.txt")
    val fwres = new FileWriter(path + personalName + "_personal_r.txt")

    fwres.write(sbRes.mkString)
    fwPersonal.write(sbPers.mkString)

    fwPersonal.close()
    fwres.close()
    System.gc()
    resultPersonal
  }

  def processData() = FileSystemHelper
    .listDirs(DataLoader.dataDir)
    .map(
      dir => (dir.getAbsolutePath, DataLoader.loadTrain(dir.getName), DataLoader.loadTest(dir.getName)))
    .filter(x => inAllRange(x))
    .map(x => (x._2, x._3))

  /*def compareCommonWithPersonal(model: RankingModel, personalName: String): Unit = {
    val data = processData()

    val resultCommon = getCommonResults(model, data)
    val resultPersonal = getPersonalResults(data, personalName)
    printMetrics(resultCommon)
    printMetricsP(resultPersonal)

    val comparisonData =
      resultCommon.zip(resultPersonal).map{
        case (common: (Double, Double, Double, Double, Double, Int),
        personal: (Double, Double, Double, Double, Double, Int, Int)) => (
          common._1 - personal._1,
          common._2 - personal._2,
          common._3 - personal._3,
          common._4 - personal._4,
          common._5 - personal._5,
          personal._7)
      }
    println(compareModels(x => x > 0, comparisonData.map(x => (x._1, x._6))))
    println(compareModels(x => x > 0, comparisonData.map(x => (x._2, x._6))))
    println(compareModels(x => x > 0, comparisonData.map(x => (x._3, x._6))))
    println(compareModels(x => x > 0, comparisonData.map(x => (x._4, x._6))))
    println(compareModels(x => x > 0, comparisonData.map(x => (x._5, x._6))))
    println(getGraph(comparisonData.map(x => (x._1, x._6))))
    println(getGraph(comparisonData.map(x => (x._2, x._6))))
    println(getGraph(comparisonData.map(x => (x._3, x._6))))
    println(getGraph(comparisonData.map(x => (x._4, x._6))))
    println(getGraph(comparisonData.map(x => (x._5, x._6))))
  }*/

  def getGraph(list: List[(Double, Int)]) = {
    val sortedList = list.sortBy(-_._2)
    var means: List[(Double, Int)] = List.empty

    var window: List[Double] = List.empty
    for(i <- sortedList.indices) {
      window = window :+ sortedList(i)._1
      if (i % 5 == 0 && i / 5 > 1) {
        means = means :+ (window.sum / window.size, sortedList(i)._2)
      }
    }
    means
  }

  def compareModels(predicate: Double => Boolean, list: List[(Double, Int)]) = {
    val sortedByCnt = list.sortBy(_._2).map(x => (if (x._1 > 0) 1 else -1, x._2))
    var it = sortedByCnt.size - 1
    var sum = 0
    var mx = 0
    for(i <- list.indices) {
      sum += sortedByCnt(i)._1
      if (sum > mx) {
        mx = sum
        it = i
      }
      print(sum + ",")
    }
    println()
    (sortedByCnt(it)._2, it, sortedByCnt.size, sortedByCnt.last._2)
    //val commonData = list.filter(x => predicate(x._1))
    //val personalData = list.filter(x => !predicate(x._1))
    //(median(commonData.map(_._2)), median(personalData.map(_._2)))
    //(commonData.map(_._2).sum / commonData.length, personalData.map(_._2).sum / personalData.length)
  }

  def readCommonResults(model: String) =
    Source.fromFile("/Users/anastasia/Development/smartfeed/data/" + model +  "_common_r.txt")
      .getLines()
      .map(l => {
        val metrics = l.split("\t").map(_.replace(',', '.').toDouble)
        (metrics(0), metrics(1), metrics(2), metrics(3), metrics(4), metrics(5).toInt)
      }).toList

  def readPersonalResults(model: String) =
    Source.fromFile("/Users/anastasia/Development/smartfeed/data/" + model + "_personal_r.txt")
      .getLines()
      .map(l => {
        val metrics = l.split("\t").map(_.replace(',', '.').toDouble)
        (metrics(0), metrics(1), metrics(2), metrics(3), metrics(4), metrics(5).toInt, metrics(6).toInt)
      }).toList

  // Helper functions

  private def getModelByName(name: String) = name match {
    case "ListNet" => new ListNetModel
    case "ListMLE" => new ListMLEModel
    case "LogReg" => new LogisticRegression
    case "RankSVM" => new RankSVM
    case _ => new ListNetModel
  }
  private def inPersonalRange(x: (String, List[Sample], List[FeatureVector])) =
    x._2.head.data.size > 300 &&
      x._2.head.data.size < 750 &&
      x._3.length > 50 &&
      x._3.map(_.label).exists(x => x > 0.0)

  private def inAllRange(x: (String, List[Sample], List[FeatureVector])) =
    x._2.head.data.size > 50 &&
      x._3.length > 50 &&
      x._3.map(_.label).exists(x => x > 0.0)

  def median(data : List[Double]) = {
    val (lower, upper) = data.sortWith(_<_).splitAt(data.size / 2)
    if (data.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head.toDouble
  }

  def getMetrics(scorePerm: List[Int], labels: List[Int]) =
    List(5, 10, 30, 50).map(x => (x, Metrics.NDCG(x, scorePerm, labels))) :+ (-1, Metrics.AP(scorePerm, labels))

  def printMetricsP(result: List[(Double, Double, Double, Double, Double, Int, Int)]) =
    printMetrics(result.map(x => (x._1, x._2, x._3, x._4, x._5, x._6)))

  def printMetrics(result: List[(Double, Double, Double, Double, Double, Int)]) = {
    println(f"Mean: NDCG@5: ${result.map(_._1).sum / result.length}%1.4f" +
      f"   NDCG@10: ${result.map(_._2).sum / result.length}%1.4f" +
      f"   NDCG@30: ${result.map(_._3).sum / result.length}%1.4f" +
      f"   NDCG@50: ${result.map(_._4).sum / result.length}%1.4f" +
      f"   MAP: ${Metrics.MAP(result.map(_._5))}%1.4f")

    println(f"Median: NDCG@5: ${median(result.map(_._1))}%1.4f" +
      f"   NDCG@10: ${median(result.map(_._2))}%1.4f" +
      f"   NDCG@30: ${median(result.map(_._3))}%1.4f" +
      f"   NDCG@50: ${median(result.map(_._4))}%1.4f")
  }

  def stats(data: List[(List[Sample], List[FeatureVector])]) = {
    val whites = data.map(x => x._1.head.data)//.map(user => user.count(_.label >= 1) / user.size.toDouble)

    var features: mutable.MutableList[Double] = mutable.MutableList.fill(whites.head.head.fvals.size)(0.0)
    for(x <- whites) {
      val f: mutable.MutableList[Double] = mutable.MutableList.fill(whites.head.head.fvals.size)(0.0)
      for (fv <- x) {
        for (i <- fv.fvals.indices) {
          f(i) += fv.fvals(i)
        }
      }
      features = features.zip(f).map{case (el, e) => el + e / x.size}
    }
    println(features.map(x => x / whites.size))
  }
  //7, 8, 9, 13,  / 5, 6, 10

  /*def stats(data: List[(List[Sample], List[FeatureVector])]) = {
    val whites = data.map(x => x._1.head.data)//.map(user => user.count(_.label >= 1) / user.size.toDouble)

    var features: mutable.MutableList[Double] = mutable.MutableList.fill(whites.head.head.fvals.size)(0.0)
    for(x <- whites) {
      val f: mutable.MutableList[Double] = mutable.MutableList.fill(whites.head.head.fvals.size)(0.0)
      for (fv <- x) {
        for (i <- fv.fvals.indices) {
          f(i) += fv.fvals(i)
        }
      }
      features = features.zip(f).map{case (el, e) => el + e / x.size}
    }
    println(features.map(x => x / whites.size))
  }*/
  //7, 8, 9, 13,  / 5, 6, 10


  def testOhsumed(): Unit = {
    val train = OhsumedParser.parse("/Users/anastasia/Development/smartfeed/src/data/1/train.txt")
    val test = OhsumedParser.parseTest("/Users/anastasia/Development/smartfeed/src/data/1/test.txt")
    runModel(new ListNetModel, train, test)
    runModel(new ListMLEModel, train, test)
  }

  def runModel(model: RankingModel, train: List[Sample], test: List[FeatureVector], path: String = "") = {
    model.trainModel(train)
    model.rank(test, path)
  }
}
