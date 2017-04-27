import containers.{FeatureVector, Sample}
import evaluation.Metrics
import models.{ListMLEModel, ListNetModel}
import parsing._

import scala.collection.mutable

/**
  * Created by anastasia.sulyagina
  */
object Main {
  val commonModel = new ListNetModel

  def main(args: Array[String]): Unit = {
    //FeatureExtractor.loadData()
    println("Grouping")
    //DataTransformer.groupByUsers()
    println("Running model")
    testAllUsers()
    //print(data.toString())
    //testOhsumed()
  }

  def testAllUsers(): Unit = {
    val data =
      FileSystemHelper
        .listDirs(DataLoader.dataDir)
        .map(
          dir => (dir.getAbsolutePath, DataLoader.loadTrain(dir.getName), DataLoader.loadTest(dir.getName)))
        .filter(x => x._2.head.data.length > 100 && x._3.length > 50 && x._3.map(_.label).exists(x => x > 0.0))
        .map(x => (x._2, x._3))
    val train = data.flatMap(x => x._1)
    val test = data.map(x => x._2)
    val listNet = new ListNetModel
    println("Training model")
    listNet.trainModel(train)
    println("Testing model")
    val result = test.map(fv => {
      val res = listNet.rank("all", fv)
      (res(0), res(1), res(2), res(3), res(4))
    })
    println(f"NDCG@5: ${result.map(_._1).sum / result.length}%1.4f" +
      f"   NDCG@10: ${result.map(_._2).sum / result.length}%1.4f" +
      f"   NDCG@30: ${result.map(_._3).sum / result.length}%1.4f" +
      f"   NDCG@50: ${result.map(_._4).sum / result.length}%1.4f" +
      f"   MAP: ${Metrics.MAP(result.map(_._5))}%1.4f")
  }

  def testUsers(): Unit = {
    val result: List[(Double, Double, Double, Double, Double)] =
      FileSystemHelper
      .listDirs(DataLoader.dataDir)
      .map(
        dir => (dir.getAbsolutePath, DataLoader.loadTrain(dir.getName), DataLoader.loadTest(dir.getName)))
      .filter(x => x._2.head.data.length > 100 && x._3.length > 50 && x._3.map(_.label).exists(x => x > 0.0))
      .map{
        case (path, train, test) =>
          val res = withListNet(train, test, path)
          (res(0), res(1), res(2), res(3), res(4))
    }
    println(f"NDCG@5: ${result.map(_._1).sum / result.length}%1.4f" +
        f"   NDCG@10: ${result.map(_._2).sum / result.length}%1.4f" +
        f"   NDCG@30: ${result.map(_._3).sum / result.length}%1.4f" +
        f"   NDCG@50: ${result.map(_._4).sum / result.length}%1.4f" +
        f"   MAP: ${Metrics.MAP(result.map(_._5))}%1.4f")
  }

  def testOhsumed(): Unit = {
    val train = OhsumedParser.parse("/Users/anastasia/Development/smartfeed/src/data/1/train.txt")
    val test = OhsumedParser.parseTest("/Users/anastasia/Development/smartfeed/src/data/1/test.txt")
    withListMLE(train, test)
    withListNet(train, test)
  }

  def withListNet(train: List[Sample], test: List[FeatureVector], path: String = "") = {
    val listNet = new ListNetModel
    println("Training model")
    listNet.trainModel(train)
    println("Testing model")
    listNet.rank(path, test)
  }

  def withListMLE(train: List[Sample], test: List[FeatureVector], path: String = "") = {
    val listMLE = new ListMLEModel
    println("Training model")
    listMLE.trainModel(train)
    println("Testing model")
    listMLE.rank(path, test)
  }
}
