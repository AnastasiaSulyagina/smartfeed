package parsing

import java.io.File

/**
  * Created by anastasia.sulyagina
  */
object FileSystemHelper {

  def listFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def listDirs(dir: String) = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isDirectory).toList
    } else {
      List[File]()
    }
  }

  def deleteAllFiles(dir: String) =
    listFiles(dir).foreach(f => f.delete())

  def deleteAllDirs(dir: String) =
    listDirs(dir).foreach(f => f.delete())

}
