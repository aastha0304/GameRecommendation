package game_recommendation

import org.apache.spark.sql.expressions.UserDefinedAggregateFunction

/**
  * Created by nik on 26/1/18.
  */

case class Payload(
                    achievement_owner_id: Option[String],
                    follower_id: Option[String],
                    game_id: Option[String],
                    source_profile_id: Option[String],
                    target_profile_id: Option[String],
                    user_id: Option[String],
                    visitor_profile_id: Option[String]
                  )

case class PlayerData(
                       event_id: Option[String],
                       event_time: Option[String],
                       event_type: Option[String],
                       payload: Option[Payload]
                     )