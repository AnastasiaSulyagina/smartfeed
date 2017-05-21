import java.io.FileWriter

import containers.{FeatureVector, Sample}
import evaluation.{Evaluation, Metrics}
import models._
import parsing._

import scala.collection.mutable
import scala.io.Source

/**
  * Created by anastasia.sulyagina
  */
object Main {
  val commonModel = new ListNetModel
  val path = "/Users/anastasia/Development/smartfeed/data/"

  def main(args: Array[String]): Unit = {

    println("Extracting features")
    //FeatureExtractor.loadData()
    println("Grouping")
    //DataTransformer.groupByUsers()
    println("Running model")

    val data = Evaluation.processData()
    println(data.size)

    //Evaluation.printMetrics(Evaluation.getCommonResults(new ListNetModel, data))
    //Evaluation.printMetricsP(Evaluation.getPersonalResults(data, "RankSVM"))
    //Evaluation.printMetricsP(Evaluation.getPersonalResults(data, "ListMLE"))

    /*val users = Source.fromFile("/Users/anastasia/Development/smartfeed/data_first/users.txt")
      .getLines()
      .flatMap(l => l.split(",").map(_.toInt)).toList
    /val data_better = data.zipWithIndex.filter{case (x, i) => users.contains(i)}.map(_._1)
    val data_worse = data.zipWithIndex.filter{case (x, i) => !users.contains(i)}.map(_._1)
    stats(data_better)
    stats(data_worse)*/
    DataLoader.toLetorFormat(data)
    //Evaluation.printMetrics(Evaluation.getCommonResults(new RankSVM, data))
    //Evaluation.printMetricsP(Evaluation.getPersonalResults(data, "ListNet"))
    /*val sbRes: StringBuilder = new StringBuilder

    Evaluation.printMetrics(data.zipWithIndex.map {
      case ((train, test), i) =>
        val scorePerm = (1 to test.size).map(_.toDouble).zipWithIndex.sortBy(x => -x._1).unzip._2.toList

        val metrics = Evaluation.getMetrics(scorePerm, test.map(_.label.toInt)).map(_._2)

        sbRes.append(metrics.map(x => f"$x%1.4f").mkString("\t") + "\t" + scorePerm.size.toString + "\t" + train.head.data.size.toString + "\n")
        //fwPersonal.write(metrics.map(x => f"$x%1.4f").mkString("\t") + "\t" + res._1.size.toString + "\t" + train.head.data.size.toString + "\n")
        (metrics(0), metrics(1), metrics(2), metrics(3), metrics(4), scorePerm.size)
    })
    //println()
    val fwres = new FileWriter(path + "Baseline" + "_personal_r.txt")

    fwres.write(sbRes.mkString)
*/
    //Evaluation.printMetrics(Evaluation.getCommonResults(new RankSVM, data))
    //Evaluation.printMetrics(Evaluation.getCommonResults(new LogisticRegression, data))
    //
    //Evaluation.printMetricsP(Evaluation.readPersonalResults("ListMLE"))

/*
    Evaluation.getPersonalResults(data, "ListMLE")
    Evaluation.getPersonalResults(data, "LogisticRegression")
    Evaluation.getPersonalResults(data, "RankSVM")
    Evaluation.getPersonalResults(data, "ListNet")
    Evaluation.getPersonalResults(data, "ListMLE")
    Evaluation.printMetrics(Evaluation.getCommonResults(new LogisticRegression, data))
    Evaluation.printMetrics(Evaluation.getCommonResults(new RankSVM, data))

    Evaluation.getCommonResults(new ListMLEModel, data)
    Evaluation.getCommonResults(new ListNetModel, data)
    */
    //Evaluation.printMetrics(Evaluation.readCommonResults("RankSVM"))
    //Evaluation.getPersonalResults(data, "ListMLE")
    // Evaluation.getPersonalResults(data, "LogisticRegression")
    //Evaluation.getPersonalResults(data, "RankSVM")

    /*data.zipWithIndex.foreach{
      case ((train, test), i) =>
        println(i.toString + "\t" + train.head.data.size)
    }*/
  }
}
