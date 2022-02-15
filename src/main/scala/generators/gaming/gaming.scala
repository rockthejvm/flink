package generators

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

/**
 * A package describing events related to a multiplayer game.
 * We analyze some essential Flink features based on these data types.
 */
package object gaming {

  sealed trait ServerEvent {
    def eventTime: Instant
    def getId: String
  }

  sealed trait GameType
  case object OneVsOne extends GameType
  case object TwoVsTwo extends GameType
  case object ThreeVsThree extends GameType
  case object FourVsFour extends GameType

  final case class GameStarted(
                                eventTime: Instant,
                                gameId: UUID,
                                playerIds: Vector[UUID],
                                mapId: String,
                                regionId: String,
                                gameType: GameType
                              ) extends ServerEvent {
    override def getId: String = s"game|$gameId"
  }

  final case class GameFinished(
                                 eventTime: Instant,
                                 gameId: UUID
                               ) extends ServerEvent {
    override def getId: String = s"game|$gameId"
  }

  final case class PlayerRegistered(
                                     eventTime: Instant,
                                     playerId: UUID,
                                     nickname: String
                                   ) extends ServerEvent {
    override def getId: String = s"player|$playerId|$nickname"
  }

  final case class PlayerOnline(
                                 eventTime: Instant,
                                 playerId: UUID,
                                 nickname: String
                               ) extends ServerEvent {
    override def getId: String = s"player|$playerId|$nickname"
  }

  final case class PlayerIsLookingForAGame(
                                            eventTime: Instant,
                                            playerId: UUID,
                                            gameType: GameType
                                          ) extends ServerEvent {
    override def getId: String = s"player|$playerId"
  }

  final case class PlayerOffline(
                                  eventTime: Instant,
                                  playerId: UUID,
                                  nickname: String
                                ) extends ServerEvent {
    override def getId: String = s"player|$playerId|$nickname"
  }

  case class Player(playerId: UUID, nickname: String) {

    def register(d: FiniteDuration)(implicit startTime: Instant): PlayerRegistered =
      PlayerRegistered(startTime.plusMillis(d.toMillis), playerId, nickname)

    def online(d: FiniteDuration)(implicit startTime: Instant): PlayerOnline =
      PlayerOnline(startTime.plusMillis(d.toMillis), playerId, nickname)

    def offline(d: FiniteDuration)(implicit startTime: Instant): PlayerOffline =
      PlayerOffline(startTime.plusMillis(d.toMillis), playerId, nickname)

    def lookingForAGame(
                         startTime: Instant,
                         d: FiniteDuration,
                         gameType: GameType
                       ): PlayerIsLookingForAGame =
      PlayerIsLookingForAGame(
        startTime.plusMillis(d.toMillis),
        playerId,
        gameType
      )
  }

  val bob: Player = Player(UUID.randomUUID(), "bob")
  val sam: Player = Player(UUID.randomUUID(), "sam")
  val rob: Player = Player(UUID.randomUUID(), "rob")
  val alice: Player = Player(UUID.randomUUID(), "alice")
  val mary: Player = Player(UUID.randomUUID(), "mary")
  val carl: Player = Player(UUID.randomUUID(), "carl")
}
