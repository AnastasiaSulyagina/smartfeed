package parsing
import java.io.File

import breeze.linalg._

import scala.io.Source

/**
  * Created by anastasia.sulyagina
  */
object DataLoader {
  def Load(path: String): DenseMatrix[Double] = {
    val data: DenseMatrix[Double] = csvread(new File("part0"))
    data
  }
}
