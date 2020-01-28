package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.event.LoggingReceive

import scala.concurrent.duration._


object Replicator {

  case class Replicate(key: String, valueOption: Option[String], id: Long)

  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  case object TimeOut

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {

  import Replicator._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  override def postStop(): Unit = if (acks.nonEmpty) acks foreach (rep => rep._2._1 ! Replicated(rep._2._2.key, rep._2._2.id))
  context.system.scheduler.scheduleAtFixedRate(Duration.Zero, 100.millisecond, self, Replicator.TimeOut)


  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pendingReps = Vector.empty[Replicate]

  var _seqCounter = 0L

  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }


  /* TODO Behavior for the Replicator. */
  def receive: Receive = LoggingReceive {
    case Replicate(key, valueOption, id)  =>
      val snapshot = Snapshot(key, valueOption, _seqCounter)
      replica ! snapshot
      val seq = nextSeq()
      acks += (seq -> ((sender(), Replicate(key, valueOption, id))))
    case SnapshotAck(key, seq) if acks contains seq =>
      acks(seq)._1 ! Replicated(key, acks(seq)._2.id)
      acks -= seq
    case Replicator.TimeOut if acks.nonEmpty =>
      acks.foreach(rep => {
        val snapshot = Snapshot(rep._2._2.key, rep._2._2.valueOption, rep._1)
        replica ! snapshot
      })

  }

}