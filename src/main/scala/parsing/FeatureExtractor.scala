package parsing

import java.io.{File, FileWriter}
import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

import language.implicitConversions

/**
  * Created by anastasia.sulyagina
  *
  * Extracting social features from initial data
  */

object FeatureExtractor {

  FileSystemHelper.deleteAllFiles("/Users/anastasia/Development/smartfeed/data/features")

  private val spark = SparkSession
    .builder()
    .appName("SmartFeed")
    .config("spark.master", "local")
    .getOrCreate()

  private val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

  /*private def getfeedbackScore(feedback: Seq[Feedback]): String =
    feedback.map(x => x.etype match {
      case Some("Commented") => 0//3 * x.count.getOrElse(1)
      case Some("ReShared") => 1
      case Some("Liked") => 1
      case Some("Clicked") => if (x.count.getOrElse(0) > 1) 1 else 0//2 * x.count.getOrElse(1)
      case Some("Disliked") => -1
      case Some("Ignored") => 0
      case Some("Viewed") => if (x.count.getOrElse(0) > 1) 1 else 0//2 * x.count.getOrElse(1)
      case _ => 0
    }).sum.toString*/

  private def getfeedbackScore(feedback: Seq[Feedback]): String =
    if (feedback.map(_.etype).contains(Some("Disliked")))
      "-1"
    else if (feedback.map(_.etype).contains(Some("Liked")))
      "1"
    else "0"
    /*feedback.map(x => x.etype match {
      case Some("Commented") => 0//3 * x.count.getOrElse(1)
      case Some("ReShared") => 1
      case Some("Liked") => 1
      case Some("Clicked") => if (x.count.getOrElse(0) > 1) 1 else 0//2 * x.count.getOrElse(1)
      case Some("Disliked") => -1
      case Some("Viewed") => if (x.count.getOrElse(0) > 1) 1 else 0//2 * x.count.getOrElse(1)
      case _ => 0
    }).max.toString*/

  private def getUserOwnerComments(counters: Option[Counters]) =
    counters match {
      case Some(interactions) =>
        (interactions.USER_STATUS_COMMENT_CREATE.getOrElse(0.0) +
          interactions.MOVIE_COMMENT_CREATE.getOrElse(0.0) +
          interactions.PHOTO_COMMENT_CREATE.getOrElse(0.0) +
          interactions.USER_PHOTO_ALBUM_COMMENT_CREATE.getOrElse(0.0)).toString
      case _ => "0"
    }

  private def getObjectType(objectType: Option[String]) = objectType match {
    case Some("Post") =>  "1,0,0"
    case Some("Photo") => "0,1,0"
    case Some("Video") => "0,0,1"
    case _ =>             "0,0,0"
  }

  private def getOwnerType(ownerType: Option[String]) = ownerType match {
    case Some("USER") =>                  "1,0,0,0,0"
    case Some("GROUP_OPEN_OFFICIAL") =>   "0,1,0,0,0"
    case Some("GROUP_OPEN") =>            "0,0,1,0,0"
    case Some("USER_APP") =>              "0,0,0,1,0"
    case Some("GROUP_CLOSED") =>          "0,0,0,0,1"
    case _ =>                             "0,0,0,0,0"
  }

  private def getGenderDiff(user: User, owner: User) =
    if (user.gender == owner.gender && user.gender.isDefined) "1" else "0"

  private def getLocationDiff(user: User, owner: User) =
    if (user.ID_Location == owner.ID_Location && user.ID_Location.isDefined) "1" else "0"

  private def getCountryDiff(user: User, owner: User) =
    if (user.ID_country == owner.ID_country && user.ID_country.isDefined) "1" else "0"

  private def getAgeDiff(user: User, owner: User) =
    if (user.birth_date.isDefined && owner.birth_date.isDefined && Math.abs(user.birth_date.get - owner.birth_date.get) / 365 < 5) "1" else "0"

  private def getFriendsReaction(weightsOpt: Option[Weights]) = weightsOpt match {
    case Some(weights) =>
      (Math.max(weights.friendLikes.getOrElse(0.0), weights.friendLikes_actors.getOrElse(0.0)) +
        Math.max(weights.friendCommentActors.getOrElse(0.0), weights.friendCommenters.getOrElse(0.0))).toString
    case _ => "0"
  }
  private def hasPolls(metadata: Metadata) = if (metadata.options.contains("HAS_POLLS")) "1" else "0"
  private def hasPhotos(metadata: Metadata) = if (metadata.options.contains("HAS_PHOTOS")) "1" else "0"
  private def hasVideos(metadata: Metadata) = if (metadata.options.contains("HAS_VIDEOS")) "1" else "0"
  private def isPartOfAlbum(metadata: Metadata) = if (metadata.options.contains("IS_PART_OF_ALBUM")) "1" else "0"
  private def hasPins(metadata: Metadata) = if (metadata.options.contains("HAS_PINS")) "1" else "0"
  private def hasText(metadata: Metadata) = if (metadata.options.contains("HAS_TEXT")) "1" else "0"
  private def isShare(metadata: Metadata) = if (metadata.options.contains("IS_INTERNAL_SHARE")) "1" else "0"
  private def isPartOfTopic(metadata: Metadata) = if (metadata.options.contains("IS_PART_OF_TOPIC")) "1" else "0"
  private def hasUrls(metadata: Metadata) = if (metadata.options.contains("HAS_URLS")) "1" else "0"
  private def isGif(metadata: Metadata) = if (metadata.options.contains("IS_GIF")) "1" else "0"

  implicit class FormattedOption(val x: Option[_]) {
    def format: String = x.getOrElse(0).toString
  }

  def firstDay = new SimpleDateFormat("dd-MM-yyyy").parse("15-11-2016").toInstant.toEpochMilli

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  // TODO: add time passed to catch more actual posts; tune friends/groups posts to see friends higher
  def getFeaturesString(entry: Entry): String =
    entry.instanceId.userId.get.toString + ',' +
  // Get day number
    ((entry.date.getTime - firstDay) / (1000*60*60*24)).toString + ',' +
    getObjectType(entry.instanceId.objectType) + ',' +
  // Weights
    getFriendsReaction(entry.audit.flatMap(_.weights)) + ',' +
    entry.audit.flatMap(_.weights).flatMap(_.ctr_high).format+ ',' +
    entry.audit.flatMap(_.weights).flatMap(_.ctr_negative).format + ',' +
    entry.audit.flatMap(_.weights).flatMap(_.ctr_gender).format + ',' +
    entry.audit.flatMap(_.weights).flatMap(_.isFavorite).format + ',' +
    //entry.audit.flatMap(_.weights).flatMap(_.x_ActorsRelations).format + ',' +
    entry.audit.flatMap(_.weights).flatMap(_.feedStats).format + ',' +
    entry.audit.flatMap(_.weights).flatMap(_.likersFeedStats_hyper).format + ',' +
    entry.audit.flatMap(_.weights).flatMap(_.relationMasks).format + ',' +
  // Metadata
    getOwnerType(entry.metadata.ownerType) + ',' +
    hasPolls(entry.metadata) + ',' +
    hasPhotos(entry.metadata) + ',' +
    hasVideos(entry.metadata) + ',' +
    isPartOfAlbum(entry.metadata) + ',' +
    hasPins(entry.metadata) + ',' +
    hasText(entry.metadata) + ',' +
    isShare(entry.metadata) + ',' +
    hasUrls(entry.metadata) + ',' +
    isGif(entry.metadata) + ',' +
  // Relations
    Relations.isArmyFellow(entry.relations.getOrElse(0)) + ',' +
    Relations.isColleague(entry.relations.getOrElse(0)) + ',' +
    Relations.isFriend(entry.relations.getOrElse(0)) + ',' +
    Relations.isFamily(entry.relations.getOrElse(0)) + ',' +
    Relations.isSchoolmate(entry.relations.getOrElse(0)) + ',' +
  // Counters
    getUserOwnerComments(entry.userOwnerCounters).toString + ',' +
    entry.userOwnerCounters.flatMap(_.PHOTO_VIEW).format + ',' +
    entry.userOwnerCounters.flatMap(_.USER_INTERNAL_LIKE).format + ',' +
    entry.userOwnerCounters.flatMap(_.USER_FEED_REMOVE).format + ',' +
    entry.userOwnerCounters.flatMap(_.USER_PROFILE_VIEW).format + ',' +
    entry.userOwnerCounters.flatMap(_.USER_SEND_MESSAGE).format + ',' +
    entry.userOwnerCounters.flatMap(_.PHOTO_MARK_CREATE).format + ',' +
  // Demography
    getGenderDiff(entry.user, entry.owner) + ',' +
    getLocationDiff(entry.user, entry.owner) + ',' +
    //getCountryDiff(entry.user, entry.owner) + ',' +
    entry.owner.is_abused.getOrElse(0) + ',' +
    getAgeDiff(entry.user, entry.owner) + ',' +
    getfeedbackScore(entry.feedback.get) + '\n'

  def loadData(dataDir: String = "/Users/anastasia/MailCloud/2/") =
    spark
      .readStream
      .schema(spark.read.load(dataDir + "date=2016-11-15").schema) // 15
      .parquet(dataDir)
      .filter(x => Parser.getId(x) % 77 == 1 && Parser.isFeedbackPresent(x))
      .writeStream.foreach(
      new ForeachWriter[Row] {
        val filePath = "/Users/anastasia/Development/smartfeed/data/features/"
        var fw: FileWriter = null

        def open(partitionId: Long, version: Long): Boolean = {
          fw = new FileWriter(s"${filePath}part_$partitionId.csv")
          true
        }

        def process(record: Row) = {
          val entry: Entry = Parser.getEntry(record)
          fw.write(getFeaturesString(entry))
        }

        def close(errorOrNull: Throwable): Unit = {
          fw.close()
        }
      })
      .start().processAllAvailable()
}
