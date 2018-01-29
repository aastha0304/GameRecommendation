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
}
