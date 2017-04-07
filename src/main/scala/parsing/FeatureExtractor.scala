package parsing

import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

/**
  * Created by anastasia.sulyagina
  */

object FeatureExtractor {
  val dataDir = "/Users/anastasia/MailCloud/dataset/"

  val spark = SparkSession
    .builder()
    .appName("SmartFeed")
    .config("spark.master", "local")
    .getOrCreate()

  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

  def getfeedbackScore(feedback: Seq[Feedback]): Int =
    feedback.map(x => x.etype match {
      case Some("Commented") => 4 * x.count.getOrElse(1)
      case Some("ReShared") => 6
      case Some("Liked") => 3
      case Some("Clicked") => 3 * x.count.getOrElse(1)
      case Some("Disliked") => -5
      case Some("Ignored") => 0
      case Some("Viewed") => 1 * x.count.getOrElse(1)
      case _ => 0
    }).sum

  def getUserOwnerComments(counters: Option[Counters]) =
    counters match {
      case Some(interactions) =>
        interactions.USER_STATUS_COMMENT_CREATE.getOrElse(0.0) +
          interactions.MOVIE_COMMENT_CREATE.getOrElse(0.0) +
          interactions.PHOTO_COMMENT_CREATE.getOrElse(0.0) +
          interactions.USER_PHOTO_ALBUM_COMMENT_CREATE.getOrElse(0.0)
      case _ => 0.0
    }

  def getObjectType(objectType: Option[String]) = objectType match {
    case Some("Post") =>  "1,0,0"
    case Some("Photo") => "0,1,0"
    case Some("Video") => "0,0,1"
    case _ =>             "0,0,0"
  }

  def getOwnerType(ownerType: Option[String]) = ownerType match {
    case Some("USER") =>                  "1,0,0,0,0"
    case Some("GROUP_OPEN_OFFICIAL") =>   "0,1,0,0,0"
    case Some("GROUP_OPEN") =>            "0,0,1,0,0"
    case Some("USER_APP") =>              "0,0,0,1,0"
    case Some("GROUP_CLOSED") =>          "0,0,0,0,1"
    case _ =>                             "0,0,0,0,0"
  }

  def getGenderDiff(user: User, owner: User) = if (user.gender == owner.gender && user.gender.isDefined) "1" else "0"

  def getLocationDiff(user: User, owner: User) = if (user.ID_Location == owner.ID_Location && user.ID_Location.isDefined) "1" else "0"

  def getCountryDiff(user: User, owner: User) = if (user.ID_country == owner.ID_country && user.ID_country.isDefined) "1" else "0"

  def getAgeDiff(user: User, owner: User) =
    if (user.birth_date.isDefined && owner.birth_date.isDefined && Math.abs(user.birth_date.get - owner.birth_date.get) / 365 < 5) "1" else "0"

  def getFriendsReaction(weightsOpt: Option[Weights]) = weightsOpt match {
    case Some(weights) =>
      (Math.max(weights.friendLikes.getOrElse(0.0), weights.friendLikes_actors.getOrElse(0.0)) +
        Math.max(weights.friendCommentActors.getOrElse(0.0), weights.friendCommenters.getOrElse(0.0))).toString
    case _ => "0.0"
  }
  def hasPolls(metadata: Metadata) = if (metadata.options.contains("HAS_POLLS")) "1" else "0"
  def hasPhotos(metadata: Metadata) = if (metadata.options.contains("HAS_PHOTOS")) "1" else "0"
  def hasVideos(metadata: Metadata) = if (metadata.options.contains("HAS_VIDEOS")) "1" else "0"
  def isPartOfAlbum(metadata: Metadata) = if (metadata.options.contains("IS_PART_OF_ALBUM")) "1" else "0"
  def hasPins(metadata: Metadata) = if (metadata.options.contains("HAS_PINS")) "1" else "0"
  def hasText(metadata: Metadata) = if (metadata.options.contains("HAS_TEXT")) "1" else "0"
  def isShare(metadata: Metadata) = if (metadata.options.contains("IS_INTERNAL_SHARE")) "1" else "0"
  def isPartOfTopic(metadata: Metadata) = if (metadata.options.contains("IS_PART_OF_TOPIC")) "1" else "0"
  def hasUrls(metadata: Metadata) = if (metadata.options.contains("HAS_URLS")) "1" else "0"
  def isGif(metadata: Metadata) = if (metadata.options.contains("IS_GIF")) "1" else "0"


  def getFeaturesString(entry: Entry): String =
    entry.instanceId.userId.getOrElse("Nan").toString + ',' +
    getObjectType(entry.instanceId.objectType) + ',' +
          //entry.audit.flatMap(_.timePassed).getOrElse("Nan") + ',' +
  // Weights
      getFriendsReaction(entry.audit.flatMap(_.weights)) + ',' +
      entry.audit.flatMap(_.weights).flatMap(_.ctr_high).getOrElse("0.0") + ',' +
      entry.audit.flatMap(_.weights).flatMap(_.ctr_negative).getOrElse("0.0") + ',' +
      entry.audit.flatMap(_.weights).flatMap(_.ctr_gender).getOrElse("0.0") + ',' +
      entry.audit.flatMap(_.weights).flatMap(_.isFavorite).getOrElse("0.0") + ',' +
      entry.audit.flatMap(_.weights).flatMap(_.x_ActorsRelations).getOrElse("0.0") + ',' +
      entry.audit.flatMap(_.weights).flatMap(_.feedStats).getOrElse(0.0) + ',' +
      entry.audit.flatMap(_.weights).flatMap(_.likersFeedStats_hyper).getOrElse(0.0).toString + ',' +
      entry.audit.flatMap(_.weights).flatMap(_.relationMasks).getOrElse(0.0).toString + ',' +
  // Metadata
      getOwnerType(entry.metadata.ownerType) + ',' +
      hasPolls(entry.metadata) + ',' +
      hasPhotos(entry.metadata) + ',' +
      hasVideos(entry.metadata) + ',' +
      isPartOfAlbum(entry.metadata) + ',' +
      hasPins(entry.metadata) + ',' +
      hasText(entry.metadata) + ',' +
      isShare(entry.metadata) + ',' +
      isPartOfTopic(entry.metadata) + ',' +
      hasUrls(entry.metadata) + ',' +
      isGif(entry.metadata) + ',' +
  // Relations
      Relations.isArmyFellow(entry.relations.getOrElse(0)) + ',' +
      Relations.isColleague(entry.relations.getOrElse(0)) + ',' +
      Relations.isFriend(entry.relations.getOrElse(0)) + ',' +
      Relations.isFamily(entry.relations.getOrElse(0)) + ',' +
      Relations.isSchoolmate(entry.relations.getOrElse(0)) + ',' +
  // Counters
      getUserOwnerComments(entry.userOwnerCounters) + ',' +
      entry.userOwnerCounters.flatMap(_.PHOTO_VIEW).getOrElse(0.0) + ',' +
      entry.userOwnerCounters.flatMap(_.USER_INTERNAL_LIKE).getOrElse(0.0) + ',' +
      entry.userOwnerCounters.flatMap(_.USER_FEED_REMOVE).getOrElse(0.0) + ',' +
      entry.userOwnerCounters.flatMap(_.USER_PROFILE_VIEW).getOrElse(0.0) + ',' +
      entry.userOwnerCounters.flatMap(_.USER_SEND_MESSAGE).getOrElse(0.0) + ',' +
      entry.userOwnerCounters.flatMap(_.PHOTO_MARK_CREATE).getOrElse(0.0) + ',' +
  // Demography
      getGenderDiff(entry.user, entry.owner) + ',' +
      getLocationDiff(entry.user, entry.owner) + ',' +
      getCountryDiff(entry.user, entry.owner) + ',' +
      entry.owner.is_abused.getOrElse(0) + ',' +
      getAgeDiff(entry.user, entry.owner) + ',' +
      getfeedbackScore(entry.feedback.get) + '\n'

  def loadData() = {
    val schema = spark.read.load(dataDir + "date=0").schema

    spark
      .readStream
      .schema(schema)
      .parquet(dataDir + "date=0")
      .filter(x => Parser.getId(x) % 107 == 1 && Parser.isFeedbackPresent(x))
      .writeStream.foreach(
      new ForeachWriter[Row] {
        val filePath = "/Users/anastasia/Development/smartfeed/data/"
        var fw: FileWriter = null

        def open(partitionId: Long, version: Long): Boolean = {
          fw = new FileWriter(s"${filePath}part_$partitionId.csv", true)
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
}
