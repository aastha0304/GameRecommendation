package game_recommendation

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by nik on 23/1/18.
  */
object GameRecommendation {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.Dataset

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Game Recommendation")
      .config("spark.master", "local")
      .getOrCreate()

  val gameEventScores = Map("GameRoundStarted" -> 1, "GameRoundEnded" -> 1, "GameRoundDropped" -> -20,
    "GameWinEvent" -> 3,
    "MajorWinEvent" -> 7)
  val playerEventScores = Map("AchievementLikedEvent" -> 1,
    "AchievementCommentedEvent" -> 2,
    "AchievementSharedEvent" -> 2,
    "ProfileVisitedEvent" -> 5,
    "DirectMessageSentEvent" -> 3)
  val timing = new StringBuffer

  /** Main function */
  def main(args: Array[String]): Unit = {
    gameRecommendation()
    spark.stop
  }

  def gameRecommendation(): Unit = {
    import spark.sqlContext.implicits._

    val gameData = spark.read.json("src/main/resources/game_recommendation/game_data")
      .select($"event_type", $"payload.game_id", $"payload.user_id")
      .where($"event_type".isin(gameEventScores.keys.toSeq: _*))
    val pgl = buildPGL(gameData)
    pgl.persist
    broadcast(pgl)

    val playerData = spark.read.json("src/main/resources/game_recommendation/player_data")
      .where($"event_type".isin(playerEventScores.keys.toSeq: _*))
      .as[PlayerData]
    val pis = buildPIS(playerData).persist

    val grvLevel1 = pis
      .join(pgl, pis("_2") === pgl("_1"), "rightouter")
      .toDF("player", "influencee", "scoreInfluence", "influenceePlayer", "game", "scoreGame")
    val grv = buildGRV(grvLevel1, pgl)

    val playerByScore = Window.partitionBy($"player")
    val rankDesc = row_number().over(playerByScore.orderBy($"joinedScore".desc)).alias("rank_desc")
    grv.select($"*", rankDesc).filter($"rank_desc" <= 6).show
  }

  def buildPGL(gameData: DataFrame) = {
    import spark.sqlContext.implicits._
    gameData
      .map(func = row => (row.getAs[String]("user_id"), row.getAs[String]("game_id"),
        gameEventScores(row.getAs[String]("event_type"))))
      .groupBy($"_1", $"_2")
      .agg(expr("sum(_3) as score"))
  }

  def buildPIS(playerData: Dataset[PlayerData]) = {
    import spark.sqlContext.implicits._
    val playerByKey = Window.partitionBy("_1").orderBy("_3")
    val rankByTime = percent_rank().over(playerByKey)

    playerData
      .map(
        row => row.event_type.get match {
          case "AchievementLikedEvent" => (row.payload.get.achievement_owner_id,
            row.payload.get.follower_id, playerEventScores(row.event_type.get), row.event_time)
          case "AchievementCommentedEvent" => (row.payload.get.achievement_owner_id,
            row.payload.get.follower_id, playerEventScores(row.event_type.get), row.event_time)
          case "ProfileVisitedEvent" => (row.payload.get.visitor_profile_id,
            row.payload.get.user_id, playerEventScores(row.event_type.get), row.event_time)
          case "AchievementSharedEvent" => (row.payload.get.follower_id,
            row.payload.get.achievement_owner_id, playerEventScores(row.event_type.get), row.event_time)
          case "DirectMessageSentEvent" => (row.payload.get.target_profile_id,
            row.payload.get.source_profile_id, playerEventScores(row.event_type.get), row.event_time)
        }
      ).select('*, rankByTime as 'percentile)
      .select($"_1", $"_2", when($"percentile" > 0.5, $"_3" * 1.5).otherwise($"_3").as("score"))
      .groupBy($"_1", $"_2")
      .agg(expr("sum(score) as score"))
  }

  def buildGRV(grvLevel1: DataFrame, pgl: DataFrame) = {
    import spark.sqlContext.implicits._
    grvLevel1.join(pgl, grvLevel1("player") === pgl("_1") && grvLevel1("game") === pgl("_2"), "rightouter")
      .where($"player".isNotNull)
      .withColumn("scoreIpIg", col("scoreInfluence") * col("scoreGame"))
      .select($"player", $"game",
        when(col("score") > 0, col("scoreIpIg") * col("score")).otherwise(col("scoreIpIg")).as("joinedScore"))
      .groupBy("player", "game")
      .agg(expr("sum(joinedScore) as joinedScore"))
      .orderBy($"player", $"joinedScore")
  }

  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
