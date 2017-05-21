package parsing

import java.io.{File, FileWriter}

import evaluation.Evaluation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by anastasia.sulyagina
  *
  * Separating users data sorting it by time
  */
object DataTransformer {

  FileSystemHelper.deleteAllDirs("/Users/anastasia/Development/smartfeed/data/grouped")

  private val spark = SparkSession
    .builder()
    .appName("SmartFeed")
    .config("spark.master", "local")
    .getOrCreate()
  import spark.implicits._

  private val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

  def groupByUsers(dataDir: String = "/Users/anastasia/Development/smartfeed/data1") =
    print(spark
      .read
      .format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .csv(dataDir + "/features")
      .rdd
      .groupBy(x => x.getString(0).toLong)
      .map { case(id, iter) =>
        val orderedData = iter.toSeq.sortBy(x => x.getString(1).toLong)
        //val train = orderedData.filter(x => x.getString(1).toLong <= 17)
        //val test = orderedData.filter(x => x.getString(1).toLong  > 17)
        val train = orderedData.filter(x => -x.getString(1).toLong % 15  == 10)
        val test = orderedData.filter(x => -x.getString(1).toLong % 15 > 12)

        train.size
        /*println(test.size, train.size)
        if (test.size > 50 && train.size > 50) {
          val dir = new File(s"$dataDir/grouped/$id")
          dir.mkdir()
          val fwTrain = new FileWriter(s"$dataDir/grouped/$id/train.csv", true)
          val fwTest = new FileWriter(s"$dataDir/grouped/$id/test.csv", true)

          train.foreach(x => fwTrain.write(x.mkString(",") + "\n"))
          test.foreach(x => fwTest.write(x.mkString(",") + "\n"))

          fwTrain.close()
          fwTest.close()
        }*/
      }.mean())
}
