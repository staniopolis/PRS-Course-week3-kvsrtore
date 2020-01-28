package kvstore

import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
import akka.event.LoggingReceive
import kvstore.Arbiter._
import akka.pattern.{ask}

import scala.concurrent.duration._
import akka.util.Timeout

import scala.util.{Failure, Success}


object Replica {

  sealed trait Operation {
    def key: String

    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case object TimeOut

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  override def preStart(): Unit = arbiter ! Join

  var expectingSeqCounter = 0L

  val persistence = context.actorOf(persistenceProps)
  override val supervisorStrategy = OneForOneStrategy() {
    case _: PersistenceException => SupervisorStrategy.Restart
  }

  context.system.scheduler.scheduleAtFixedRate(Duration.Zero, 100.millisecond, self, Replica.TimeOut)
  implicit val timeout: Timeout = 1.second

  var acks = Map.empty[Long, (ActorRef, Persist)]
  var repAcks = Map.empty[Long, (ActorRef, Int)]

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]


  def receive: Receive = LoggingReceive {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = LoggingReceive {
    //--------------------Create replicator-----------------------------------------------------
    case Replicas(replicas) =>
      for {
        replica <- secondaries.keySet
        if !(replicas contains replica)
      } yield {
        replica ! PoisonPill
      }
      for {
        replica <- replicas
        if !(secondaries contains replica) && replica != context.self
      } yield {
        val replicator = context.actorOf(Replicator.props(replica))
        if (kv.nonEmpty) {
          var initial = kv.size
          kv foreach (value => {
            (replicator ? Replicate(value._1, Some(value._2), -1)).mapTo[Replicated] onComplete {
              case Success(_) =>
                if (initial > 1) initial -= 1
                else {
                  replicators += replicator
                  secondaries += replica -> replicator
                  context.watch(replica)
                }
              case Failure(_) =>
            }
          })
        } else {
          replicators += replicator
          secondaries += replica -> replicator
          context.watch(replica)
        }
      }
    //------------------Get--------------------------------------------
    case Get(key, id) =>
      val value = kv.get(key)
      sender() ! GetResult(key, value, id)
    //-------------------Insert----------------------------------------
    case Insert(key, value, id) =>
      kv += (key -> value)
      val persist = Persist(key, Some(value), id)
      acks += id -> ((sender(), persist))
      (persistence ? persist).mapTo[Persisted] onComplete {
        case Failure(_) if acks contains id => acks(id)._1 ! OperationFailed(id)
          acks -= id
        case Success(result) => self ! result
      }
    //--------------------Remove----------------------------------------
    case Remove(key, id) =>
      kv -= key
      val persist = Persist(key, None, id)
      acks += id -> ((sender(), persist))
      (persistence ? persist).mapTo[Persisted] onComplete {
        case Failure(_) if acks contains id => acks(id)._1 ! OperationFailed(id)
          acks -= id
        case Success(result) => self ! result
      }
    //---------------------Persisted----------------------------------------------------
    case Persisted(_, id) if acks contains id =>
      if (replicators.nonEmpty) {
        val replicate = Replicate(acks(id)._2.key, acks(id)._2.valueOption, acks(id)._2.id)
        repAcks += id -> ((acks(id)._1, replicators.size))
        acks -= id
        replicators foreach (replicator => (replicator ? replicate).mapTo[Replicated] onComplete {
          case Failure(_) if repAcks contains id => repAcks(id)._1 ! OperationFailed(id)
            acks -= id
          case Success(result) => self ! result
        })
      }
      else {
        acks(id)._1 ! OperationAck(id)
        acks -= id
      }
    //--------------------------TimeOut--------------------------------------------
    case Replica.TimeOut if acks.nonEmpty =>
      acks foreach (persistence ! _._2._2)
    //-----------------------Replicated-------------------------------------------
    case Replicated(_, id) if repAcks contains id =>
      if (repAcks(id)._2 > 1) {
        repAcks = repAcks.updated(id, (repAcks(id)._1, repAcks(id)._2 - 1))
      }
      else {
        repAcks(id)._1 ! OperationAck(id)
        repAcks -= id
      }



    //-----------------------------Secondary replica terminated0------------------------
    case msg: Terminated =>
      context.unwatch(msg.actor)
      secondaries(msg.actor) ! PoisonPill
      replicators -= secondaries(msg.actor)
      secondaries -= msg.actor
  }


  /* TODO Behavior for the replica role. */
  val replica: Receive = LoggingReceive {
    case Get(key, id) =>
      val value = kv.get(key)
      sender ! GetResult(key, value, id)
    //--------------------------------------------------------------------------------------------
    case Snapshot(key, valueOption, seq) if seq == expectingSeqCounter =>
      valueOption match {
        case Some(value) => kv += (key -> value)
        case None => kv -= key
      }
      val persist = Persist(key, valueOption, seq)
      acks += seq -> ((sender(), persist))
      persistence ! persist
      expectingSeqCounter += 1
    case Snapshot(key, _, seq) if seq < expectingSeqCounter =>
      sender() ! SnapshotAck(key, seq)
    //----------------------------------------------------------------------------
    case Replica.TimeOut if acks.nonEmpty =>
      acks.foreach(operation => persistence ! operation._2._2)
    //----------------------------------------------------------------------------
    case Persisted(key, seq) if acks contains seq =>
      acks(seq)._1 ! SnapshotAck(key, seq)
      acks -= seq
  }
}

