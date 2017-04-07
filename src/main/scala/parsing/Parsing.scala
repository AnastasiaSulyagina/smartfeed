package parsing

import org.apache.spark.sql.Row

/**
  * Created by anastasia.sulyagina
  */

object Parser {
  type A[E] = collection.mutable.WrappedArray[E]

  implicit class RichRow(val r: Row) {
    def getOpt[T](n: String): Option[T] = {
      if (isNullAt(n)) {
        None
      } else {
        Some(r.getAs[T](n))
      }
    }

    def getStringOpt(n: String) = getOpt[String](n)

    def getString(n: String) = getStringOpt(n).get

    def getIntOpt(n: String) = getOpt[Int](n)

    def getInt(n: String) = r.getIntOpt(n).get

    def getLongOpt(n: String) = getOpt[Long](n)

    def getLong(n: String) = r.getLongOpt(n).get

    def getDoubleOpt(n: String) = getOpt[Double](n)

    def getDouble(n: String) = r.getDoubleOpt(n).get

    def getArray[T](n: String) = r.getAs[A[T]](n)

    def getRow(n: String) = r.getAs[Row](n)

    def getRows(n: String) = r.getAs[A[Row]](n)

    def getRowOpt(n: String) = getOpt[Row](n)

    def isNullAt(n: String) = r.isNullAt(r.fieldIndex(n))
  }
  // Helper functions for filtering
  def getId(r: Row): Int = r.getRow("instanceId").getIntOpt("userId").getOrElse(-1)

  def isFeedbackPresent(r: Row): Boolean = !r.isNullAt("feedback")

  def getEntry(r: Row): Entry =
    new Entry(
      getInstanceId(r.getRow("instanceId")),
      getAudit(r.getRowOpt("audit")),
      getMetadata(r.getRow("metadata")),
      r.getLongOpt("relationsMask"),
      getCounters(r.getRowOpt("userOwnerCounters")),
      getCounters(r.getRowOpt("ownerUserCounters")),
      getMembership(r.getRow("membership")),
      getUser(r.getRow("owner")),
      getUser(r.getRow("user")),
      getFeedback(r)//.getRowOpt("feedback"))
    )

  def getInstanceId(r: Row): InstanceId =
    new InstanceId(
      r.getIntOpt("userId"),
      r.getStringOpt("objectType"),
      r.getIntOpt("objectId"))

  def getAudit(or: Option[Row]): Option[Audit] =
    or match {
      case Some(r) =>
        Some(new Audit(
          r.getLongOpt("pos"),
          r.getLongOpt("timestamp"),
          r.getLongOpt("timePassed"),
          getWeights(r.getRowOpt("weights"))))
      case None => None
    }

  def getWeights(or: Option[Row]): Option[Weights] =
    or match {
      case Some(r) =>
        Some(new Weights(
          r.getDoubleOpt("ageMs"),
          r.getDoubleOpt("ctr_gender"),
          r.getDoubleOpt("ctr_high"),
          r.getDoubleOpt("ctr_negative"),
          r.getDoubleOpt("dailyRecency"),
          r.getDoubleOpt("feedStats"),
          r.getDoubleOpt("friendLikes"),
          r.getDoubleOpt("friendLikes_actors"),
          r.getDoubleOpt("isFavorite"),
          r.getDoubleOpt("likersFeedStats_hyper"),
          r.getDoubleOpt("likersSvd_prelaunch_hyper"),
//          r.getDoubleOpt("matrix"),
          r.getDoubleOpt("ownerAge"),
          r.getDoubleOpt("partAge"),
          r.getDoubleOpt("partCtr"),
          r.getDoubleOpt("partSvd"),
          r.getDoubleOpt("relationMasks"),
          r.getDoubleOpt("svd_prelaunch"),
          r.getDoubleOpt("userAge"),
          r.getLongOpt("x_ActorsRelations"),
//          r.getDoubleOpt("svd_prelaunch_nobi"),
//          r.getDoubleOpt("ldaIntersection"),
          r.getDoubleOpt("friendCommentActors"),
          r.getDoubleOpt("friendCommentFeeds"),
          r.getDoubleOpt("friendCommenters")
//          r.getDoubleOpt("isPymk") - too little data, most content owners are already friends => rare case
        ))
      case None => None
    }

  def getMetadata(r: Row): Metadata =
    new Metadata(
      r.getIntOpt("ownerId"),
      r.getStringOpt("ownerType"),
      r.getLongOpt("createdAt"),
      r.getIntOpt("authorId"),
//      r.getLongOpt("applicationId"),
      r.getIntOpt("numCompanions"),
      r.getIntOpt("numPhotos"),
      r.getIntOpt("numPolls"),
      r.getIntOpt("numSymbols"),
      r.getIntOpt("numTokens"),
      r.getIntOpt("numVideos"),
//      r.getStringOpt("platform"),
//      r.getIntOpt("totalVideoLength"),
      r.getArray("options"))

  def getCounters(or: Option[Row]): Option[Counters] =
    or match {
      case Some(r) =>
        Some(new Counters(
//          r.getDoubleOpt("ignoredField"),
          r.getDoubleOpt("USER_FEED_REMOVE"),
          r.getDoubleOpt("USER_PROFILE_VIEW"),
          r.getDoubleOpt("VOTE_POLL"),
          r.getDoubleOpt("USER_SEND_MESSAGE"),
          r.getDoubleOpt("USER_DELETE_MESSAGE"),
          r.getDoubleOpt("USER_INTERNAL_LIKE"),
          r.getDoubleOpt("USER_INTERNAL_UNLIKE"),
          r.getDoubleOpt("USER_STATUS_COMMENT_CREATE"),
          r.getDoubleOpt("PHOTO_COMMENT_CREATE"),
          r.getDoubleOpt("MOVIE_COMMENT_CREATE"),
          r.getDoubleOpt("USER_PHOTO_ALBUM_COMMENT_CREATE"),
          r.getDoubleOpt("COMMENT_INTERNAL_LIKE"),
//          r.getDoubleOpt("USER_FORUM_MESSAGE_CREATE"),
          r.getDoubleOpt("PHOTO_MARK_CREATE"),
          r.getDoubleOpt("PHOTO_VIEW"),
          r.getDoubleOpt("PHOTO_PIN_BATCH_CREATE"),
          r.getDoubleOpt("PHOTO_PIN_UPDATE"),
          r.getDoubleOpt("USER_PRESENT_SEND")
        ))
      case None => None
    }

  def getMembership(r: Row): Membership =
    new Membership(
      r.getStringOpt("status"),
      r.getLongOpt("statusUpdateDate"),
      r.getLongOpt("joinDate"),
      r.getLongOpt("joinRequestDate")
    )

  def getUser(r: Row): User =
    new User(
//      r.getLongOpt("create_date"),
      r.getIntOpt("birth_date"),
      r.getIntOpt("gender"),
      r.getIntOpt("status"),
      r.getLongOpt("ID_country"),
      r.getIntOpt("ID_Location"),
//      r.getIntOpt("is_active"),
      r.getIntOpt("is_deleted"),
      r.getIntOpt("is_abused"),
//      r.getIntOpt("is_activated"),
//      r.getLongOpt("change_datime"),
//      r.getIntOpt("is_semiactivated"),
      r.getIntOpt("region")
    )

  def getFeedback(r: Row): Option[Seq[Feedback]] =
    Some(r.getRows("feedback").map(x =>
      new Feedback(
        x.getStringOpt("type"),
        x.getIntOpt("count"))))
}

case class TestEntry(audit: Option[Audit])

case class Entry(
  instanceId: InstanceId,
  audit: Option[Audit],
  metadata: Metadata,
  relations: Option[Long],
  userOwnerCounters: Option[Counters],
  ownerUserCounters: Option[Counters],
  membership: Membership,
  owner: User,
  user: User,
  feedback: Option[Seq[Feedback]]
)

case class InstanceId(
  userId: Option[Int],
  objectType: Option[String],
  objectId: Option[Int]
)

case class Audit(
  pos: Option[Long],
  timestamp: Option[Long],
  timePassed: Option[Long],
  weights: Option[Weights]
)

case class Weights(
  ageMs: Option[Double],
  ctr_gender: Option[Double],
  ctr_high: Option[Double],
  ctr_negative: Option[Double],
  dailyRecency: Option[Double], // что-то про возраст
  feedStats: Option[Double], // вес автора относительно объекта по какой-то формуле
  friendLikes: Option[Double],
  friendLikes_actors: Option[Double], // уникальные друзья
  isFavorite: Option[Double],
  likersFeedStats_hyper: Option[Double], // фидстат всех кто лайкал относительно пользователя
  likersSvd_prelaunch_hyper: 	Option[Double], // svd по тем кто лайкал
//  matrix: Option[Double], // суммарный прогноз модели
  ownerAge: Option[Double],
  partAge: Option[Double], // форвард
  partCtr: Option[Double],
  partSvd: Option[Double],
  relationMasks: Option[Double],
  svd_prelaunch: Option[Double],
  userAge: Option[Double],
  x_ActorsRelations: Option[Long], // маски отношений ксор
//  svd_prelaunch_nobi: Option[Double],
//  ldaIntersection: Option[Double], //  тематики - no data
  friendCommentActors: Option[Double],
  friendCommentFeeds: Option[Double],
  friendCommenters: Option[Double]
//  isPymk: Option[Double] // потенциальный друг
)

case class Metadata(
  ownerId: Option[Int],
  ownerType: Option[String],
  createdAt: Option[Long],
  authorId: Option[Int],
//  applicationId: Option[Long],
  numCompanions: Option[Int],
  numPhotos: Option[Int],
  numPolls: Option[Int],
  numSymbols: Option[Int],
  numTokens: Option[Int],
  numVideos: Option[Int],
//  platform: Option[String],
//  totalVideoLength: Option[Int],
  options: Seq[String] // признаки
)

case class Counters(
//   ignoredField: Option[Double],
   USER_FEED_REMOVE: Option[Double],
   USER_PROFILE_VIEW: Option[Double],
   VOTE_POLL: Option[Double],
   USER_SEND_MESSAGE: Option[Double],
   USER_DELETE_MESSAGE: Option[Double],
   USER_INTERNAL_LIKE: Option[Double],
   USER_INTERNAL_UNLIKE: Option[Double],
   USER_STATUS_COMMENT_CREATE: Option[Double],
   PHOTO_COMMENT_CREATE: Option[Double],
   MOVIE_COMMENT_CREATE: Option[Double],
   USER_PHOTO_ALBUM_COMMENT_CREATE: Option[Double],
   COMMENT_INTERNAL_LIKE: Option[Double],
//   USER_FORUM_MESSAGE_CREATE: Option[Double], //- нинада
   PHOTO_MARK_CREATE: Option[Double],
   PHOTO_VIEW: Option[Double],
   PHOTO_PIN_BATCH_CREATE: Option[Double],
   PHOTO_PIN_UPDATE: Option[Double],
   USER_PRESENT_SEND: Option[Double]
)

case class Membership(
  status: Option[String], 
  statusUpdateDate: Option[Long], 
  joinDate: Option[Long], 
  joinRequestDate: Option[Long]
)

case class User(
//  create_date: Option[Long],
  birth_date: Option[Int],
  gender: Option[Int],
  status: Option[Int],
  ID_country: Option[Long],
  ID_Location: Option[Int],
//  is_active: Option[Int],
  is_deleted: Option[Int],
  is_abused: Option[Int],
//  is_activated: Option[Int],
//  change_datime: Option[Long],
//  is_semiactivated: Option[Int], //-
  region: Option[Int]
)

case class Feedback(etype: Option[String], count: Option[Int])
