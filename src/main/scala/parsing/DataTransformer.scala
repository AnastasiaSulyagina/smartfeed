package parsing

import java.io.{File, FileWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by anastasia.sulyagina
  *
  * Separating users data sorting it by time
  */
object DataTransformer {

  FileSystemHelper.deleteAllFiles("/Users/anastasia/Development/smartfeed/data/grouped")

  private val spark = SparkSession
    .builder()
    .appName("SmartFeed")
    .config("spark.master", "local")
    .getOrCreate()
  import spark.implicits._

  private val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

  def groupByUsers(dataDir: String = "/Users/anastasia/Development/smartfeed/data") =
    spark
      .read
      .format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .csv(dataDir + "/features")
      .rdd
      .groupBy(x => x.getString(0).toLong)
      .foreach { case(id, iter) =>
        val dir = new File(s"$dataDir/grouped/$id")
        dir.mkdir()
        val fwTrain = new FileWriter(s"$dataDir/grouped/$id/train.csv", true)
        val fwTest = new FileWriter(s"$dataDir/grouped/$id/test.csv", true)

        iter.toSeq.sortBy(x => x.getString(1).toLong).foreach(
          x => if (x.getString(1).toLong % 15 > 12) fwTest.write(x.mkString(",") + "\n")
          else fwTrain.write(x.mkString(",") + "\n"))
        fwTrain.close()
        fwTest.close()
      }
}
