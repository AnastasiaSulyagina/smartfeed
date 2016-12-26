import containers.{FeatureVector, Sample}
import models.{ListMLEModel, ListNetModel}

/**
  * Created by anastasia.sulyagina
  */
object Main {
/**
  * Saving predicted data to file, to evaluate download
  *   'The Evaluation Tool in LETOR' provided by Jun Xu, Tie-Yan Liu, and Hang Li
  */
  def main(args: Array[String]): Unit = {
    val train = OhsumedParser.parse("/Users/anastasia/Development/smartfeed/src/data/1/train.txt")
    val test = OhsumedParser.parseTest("/Users/anastasia/Development/smartfeed/src/data/1/vali.txt")
    withListMLE(train, test)
    withListNet(train, test)
  }

  def withListNet(train: List[Sample], test: List[FeatureVector]) = {
    val listNet = new ListNetModel
    listNet.trainModel(train)
    listNet.rank(test)
  }

  def withListMLE(train: List[Sample], test: List[FeatureVector]) = {
    val listMLE = new ListMLEModel
    listMLE.trainModel(train)
    listMLE.rank(test)
  }
}
