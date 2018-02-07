package game_recommendation

/**
  * Created by nik on 29/1/18.
  */

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class GameRecommendationSuite extends FunSuite with BeforeAndAfterAll {

  import org.apache.spark.sql.DataFrame

  override def afterAll(): Unit = {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import GameRecommendation._
    spark.stop()
  }

  def initializeWikipediaRanking(): Boolean =
    try {
      GameRecommendation
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  def assertSmallDataFrameEquality(actualDF: DataFrame, expectedDF: DataFrame): Unit = {
    assert(actualDF.schema.equals(expectedDF.schema))
    assert(actualDF.collect().sameElements(expectedDF.collect()))
  }

  test("buildPGL should give expected results") {
    assert(initializeWikipediaRanking(), " -- something is not right")
    import GameRecommendation._
    import spark.implicits._

    val testData = Seq(
      ("GameRoundStarted", "game_1", "player_A"),
      ("GameRoundEnded", "game_1", "player_A"),
      ("GameRoundStarted", "game_1", "player_A"),
      ("GameRoundEnded", "game_1", "player_A"),
      ("GameRoundStarted", "game_1", "player_A"),
      ("GameRoundEnded", "game_1", "player_A"),
      ("GameRoundStarted", "game_1", "player_A"),
      ("GameRoundEnded", "game_1", "player_A"),
      ("GameRoundStarted", "game_1", "player_A"),
      ("GameRoundEnded", "game_1", "player_A"),

      ("GameWinEvent", "game_1", "player_A"),
      ("MajorWinEvent", "game_1", "player_A"),
      ("GameWinEvent", "game_1", "player_A"),
      ("MajorWinEvent", "game_1", "player_A"),

      ("GameRoundStarted", "game_1", "player_B"),
      ("GameRoundEnded", "game_1", "player_B"),
      ("GameWinEvent", "game_1", "player_B"),

      ("GameRoundStarted", "game_2", "player_B"),
      ("GameRoundEnded", "game_2", "player_B"),
      ("GameWinEvent", "game_2", "player_B")
    ).toDF("event_type", "game_id", "user_id")
    val pgl = buildPGL(testData)
    val expectedPgl = Seq(
      ("player_A", "game_1", 30),
      ("player_B", "game_1", 5),
      ("player_B", "game_2", 5))
      .toDF("_1", "_2", "score")
    //assertSmallDataFrameEquality(pgl, expectedPgl)
    assert(pgl.collect().sameElements(expectedPgl.collect()))
  }

  test("buildPIS should give expected results") {
    assert(initializeWikipediaRanking(), " -- something is not right")
    import GameRecommendation._
    import spark.implicits._
    import org.apache.spark.sql.Dataset

    val testData = Seq(
      PlayerData(Some("1397ab9e-ac6f-49f5-a2da-d8fa20d0a101"), Some("2017-08-01 00:00:00"), Some("ProfileVisitedEvent"),
        Some(Payload(None, None, None, None, None, Some("player_B"), Some("player_A")))),
      PlayerData(Some("1397ab9e-ac6f-49f5-a2da-d8fa20d0a101"), Some("2017-08-02 01:32:00"), Some("ProfileVisitedEvent"),
        Some(Payload(None, None, None, None, None, Some("player_B"), Some("player_A")))),
      PlayerData(Some("1397ab9e-ac6f-49f5-a2da-d8fa20d0a101"), Some("2017-08-03 02:42:00"), Some("ProfileVisitedEvent"),
        Some(Payload(None, None, None, None, None, Some("player_B"), Some("player_A")))),
      PlayerData(Some("1397ab9e-ac6f-49f5-a2da-d8fa20d0a101"), Some("2017-08-04 04:13:00"), Some("ProfileVisitedEvent"),
        Some(Payload(None, None, None, None, None, Some("player_B"), Some("player_A")))),
      PlayerData(Some("1397ab9e-ac6f-49f5-a2da-d8fa20d0a101"), Some("2017-08-05 13:00:14"), Some("ProfileVisitedEvent"),
        Some(Payload(None, None, None, None, None, Some("player_B"), Some("player_A")))),
      PlayerData(Some("1397ab9e-ac6f-49f5-a2da-d8fa20d0a101"), Some("2017-08-06 00:00:00"), Some("ProfileVisitedEvent"),
        Some(Payload(None, None, None, None, None, Some("player_B"), Some("player_A")))),
      PlayerData(Some("1397ab9e-ac6f-49f5-a2da-d8fa20d0a101"), Some("2017-08-07 01:32:00"), Some("ProfileVisitedEvent"),
        Some(Payload(None, None, None, None, None, Some("player_B"), Some("player_A")))),
      PlayerData(Some("1397ab9e-ac6f-49f5-a2da-d8fa20d0a101"), Some("2017-08-08 02:42:00"), Some("ProfileVisitedEvent"),
        Some(Payload(None, None, None, None, None, Some("player_B"), Some("player_A")))),
      PlayerData(Some("1397ab9e-ac6f-49f5-a2da-d8fa20d0a101"), Some("2017-08-09 04:13:00"), Some("ProfileVisitedEvent"),
        Some(Payload(None, None, None, None, None, Some("player_B"), Some("player_A")))),
      PlayerData(Some("1397ab9e-ac6f-49f5-a2da-d8fa20d0a101"), Some("2017-08-10 13:00:14"), Some("ProfileVisitedEvent"),
        Some(Payload(None, None, None, None, None, Some("player_B"), Some("player_A")))),
      PlayerData(Some("1397ab9e-ac6f-49f5-a2da-d8fa20d0a111"), Some("2017-08-03 17:00:00"), Some("AchievementLikedEvent"),
        Some(Payload(Some("player_B"), Some("player_A"), Some("game_1"), None, None, None, None)))
    ).toDF.as[PlayerData]
    val pis = buildPIS(testData)
    val expectedPis = Seq(
      ("player_A", "player_B", 50.0),
      ("player_B", "player_A", 1.0))
      .toDF("_1", "_2", "score")
    //assertSmallDataFrameEquality(pgl, expectedPgl)
    assert(pis.collect().sameElements(expectedPis.collect()))
  }

  test("buildGRV should give expected results") {
    assert(initializeWikipediaRanking(), " -- something is not right")
    import GameRecommendation._
    import spark.implicits._
    val testDataGrv = Seq(
      ("player_A", "player_C", 50.0, "player_C", "game_1", 7),
      ("player_C", "player_A", 7.5, "player_A", "game_1", 30),
      ("player_B", "player_A", 96.5, "player_A", "game_1", 30),
      ("player_C", "player_B", 7.0, "player_B", "game_3", 10),
      ("player_A", "player_B", 55.0, "player_B", "game_3", 10),
      ("player_C", "player_B", 7.0, "player_B", "game_1", 5),
      ("player_A", "player_B", 55.0, "player_B", "game_1", 5),
      ("player_C", "player_B", 7.0, "player_B", "game_2", 10),
      ("player_A", "player_B", 55.0, "player_B", "game_2", 10           )
    ).toDF("player", "influencee", "scoreInfluence", "influencePlayer", "game", "scoreGame")

    val testDataPgl = Seq(
      ("player_A", "game_1", 30),
      ("player_B", "game_1", 5),
      ("player_B", "game_2", 10),
      ("player_B", "game_3", 10),
      ("player_C", "game_1", 7)
    ).toDF("_1", "_2", "score")

    val grv = buildGRV(testDataGrv, testDataPgl)
    val expectedGrv = Seq(
      ("player_A", "game_1", 18750.0),
      ("player_B", "game_1", 14475.0),
      ("player_C", "game_1", 1820.0))
      .toDF("player", "game", "joinedScore")
    //assertSmallDataFrameEquality(pgl, expectedPgl)
    assert(grv.collect().sameElements(expectedGrv.collect()))
  }
}
