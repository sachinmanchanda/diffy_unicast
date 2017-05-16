package com.twitter.diffy.proxy

import javax.inject.Singleton
import com.google.inject.Provides
import com.twitter.diffy.analysis._
import com.twitter.diffy.lifter.Message
import com.twitter.finagle._
import com.twitter.inject.TwitterModule
import com.twitter.logging.Logger
import com.twitter.util._
import scala.collection.IndexedSeq

object DifferenceProxyModule extends TwitterModule {
  @Provides
  @Singleton
  def providesDifferenceProxy(
    settings: Settings,
    collector: InMemoryDifferenceCollector,
    joinedDifferences: JoinedDifferences,
    analyzer: DifferenceAnalyzer): DifferenceProxy =
    settings.protocol match {
      case "thrift" => ThriftDifferenceProxy(settings, collector, joinedDifferences, analyzer)
      case "http" => SimpleHttpDifferenceProxy(settings, collector, joinedDifferences, analyzer)
      case "https" => SimpleHttpsDifferenceProxy(settings, collector, joinedDifferences, analyzer)
    }
}

object DifferenceProxy {
  object NoResponseException extends Exception("No responses provided by diffy")
  val NoResponseExceptionFuture = Future.exception(NoResponseException)
  val log = Logger(classOf[DifferenceProxy])
}

trait DifferenceProxy {
  import DifferenceProxy._

  type Req
  type Rep
  type Srv <: ClientService[Req, Rep]

  val server: ListeningServer
  val settings: Settings
  var lastReset: Time = Time.now

  def serviceFactory(serverset: String, label: String): Srv

  def liftRequest(req: Req): Future[Message]
  def liftResponse(rep: Try[Rep]): Future[Message]

  // Clients for services
  val candidate = serviceFactory(settings.candidate.path, "candidate")
  val primary = serviceFactory(settings.primary.path, "primary")
  val secondary = serviceFactory(settings.secondary.path, "secondary")

  val collector: InMemoryDifferenceCollector

  val joinedDifferences: JoinedDifferences

  val analyzer: DifferenceAnalyzer
  /*
  private[this] lazy val multicastHandler =
    new SequentialMulticastService(Seq(primary.client, candidate.client, secondary.client))

  def proxy = new Service[Req, Rep] {
    override def apply(req: Req): Future[Rep] = {
      val rawResponses =
        multicastHandler(req) respond {
          case Return(_) => log.debug("success networking")
          case Throw(t) => log.debug(t, "error networking")
        }

      val responses: Future[Seq[Message]] =
        rawResponses flatMap { reps =>
          Future.collect(reps map liftResponse) respond {
            case Return(rs) =>
              log.debug(s"success lifting ${rs.head.endpoint}")

            case Throw(t) => log.debug(t, "error lifting")
          }
        }

      responses foreach {
        case Seq(primaryResponse, candidateResponse, secondaryResponse) =>
          liftRequest(req) respond {
            case Return(m) =>
              log.debug(s"success lifting request for ${m.endpoint}")

            case Throw(t) => log.debug(t, "error lifting request")
          } foreach { req =>
            analyzer(req, candidateResponse, primaryResponse, secondaryResponse)
          }
      }

      NoResponseExceptionFuture*/
  private[this] lazy val multicastHandlerPrimary =
    new SequentialMulticastService(Seq(primary.client))

  private[this] lazy val multicastHandlerCandidate =
    new SequentialMulticastService(Seq(candidate.client))

  private[this] lazy val multicastHandlerSecondary =
    new SequentialMulticastService(Seq(secondary.client))

  var requestNo: Int = 0

  var responsesPrimary: Future[Seq[Message]] = Future.Nil

  var responsesCandidate: Future[Seq[Message]] = Future.Nil

  var responsesSecondary: Future[Seq[Message]] = Future.Nil
  
  var response  = scala.collection.mutable.Map[Int, IndexedSeq[Future[Seq[Message]]]]()

  def proxy = new Service[Req, Rep] {
    override def apply(req: Req): Future[Rep] = {
      var currentResponse: Future[Seq[Try[Rep]]] = Future.Nil
      requestNo = (requestNo + 1) % 3
      if (requestNo == 1) {
        val rawResponsesPrimaryF = multicastHandlerPrimary(req) respond {
          case Return(_) => log.debug("success networking")
          case Throw(t) => log.debug(t, "error networking")
        }

        Await.result(rawResponsesPrimaryF)
        responsesPrimary =
          rawResponsesPrimaryF flatMap { reps =>
            Future.collect(reps map liftResponse) respond {
              case Return(rs) =>
                log.info(s"success lifting ${rs.head.endpoint}")

              case Throw(t) => log.info(t, "error lifting")
            }
          }
        currentResponse = rawResponsesPrimaryF

      } else if (requestNo == 2) {
        val rawResponsesCandidateF =
          multicastHandlerCandidate(req) respond {
            case Return(_) => log.debug("success networking")
            case Throw(t) => log.debug(t, "error networking")
          }
        Await.result(rawResponsesCandidateF)
        responsesCandidate =
          rawResponsesCandidateF flatMap { reps =>
            Future.collect(reps map liftResponse) respond {
              case Return(rs) =>
                log.info(s"success lifting ${rs.head.endpoint}")

              case Throw(t) => log.info(t, "error lifting")
            }
          }
        currentResponse = rawResponsesCandidateF

      } else if (requestNo == 0) {
        val rawResponsesSecondaryF =
          multicastHandlerSecondary(req) respond {
            case Return(_) => log.debug("success networking")
            case Throw(t) => log.debug(t, "error networking")
          }
        Await.result(rawResponsesSecondaryF)
        responsesSecondary =
          rawResponsesSecondaryF flatMap { reps =>
            Future.collect(reps map liftResponse) respond {
              case Return(rs) =>
                log.info(s"success lifting ${rs.head.endpoint}")

              case Throw(t) => log.info(t, "error lifting")
            }
          }
        currentResponse = rawResponsesSecondaryF
      }

      log.info("Request number : " + requestNo)
      log.info("rawResponsesPrimary " + responsesPrimary.get().size)
      log.info("rawResponsesCandidate " + responsesCandidate.get().size)
      log.info("rawResponsesSecondary " + responsesSecondary.get().size)
      log.info("currentResponse " + currentResponse)

      if (requestNo == 0) {
        val responses: Future[Seq[Message]] = Future.value(Seq(responsesPrimary.get().head, responsesCandidate.get().head, responsesSecondary.get().head))
        log.info("Raw response : " + responses.get().toString())
        responses foreach {
          case Seq(primaryResponse, candidateResponse, secondaryResponse) =>
            liftRequest(req) respond {
              case Return(m) =>
                log.info(s"success lifting request for ${m.endpoint}")

              case Throw(t) => log.info(t, "error lifting request")
            } foreach { req =>
              analyzer(req, candidateResponse, primaryResponse, secondaryResponse)
            }
        }
      }

      Future.value(currentResponse.get().head.get())
    }
  }

  def clear() = {
    lastReset = Time.now
    analyzer.clear()
  }
}
