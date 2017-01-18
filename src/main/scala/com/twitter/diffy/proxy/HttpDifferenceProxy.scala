package com.twitter.diffy.proxy

import java.net.SocketAddress

import com.twitter.diffy.analysis.{ DifferenceAnalyzer, JoinedDifferences, InMemoryDifferenceCollector }
import com.twitter.diffy.lifter.{ HttpLifter, Message }
import com.twitter.diffy.proxy.DifferenceProxy.NoResponseException
import com.twitter.finagle.{ Service, Http, Filter }
import com.twitter.finagle.http.{ Status, Response, Method, Request }
import com.twitter.util.{ Try, Future }
import org.jboss.netty.handler.codec.http.{ HttpResponse, HttpRequest }
import com.twitter.util._
import com.twitter.logging.Logger
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

object HttpDifferenceProxy {
  val okResponse = Future.value(Response(Status.Ok))

  val noResponseExceptionFilter =
    new Filter[HttpRequest, HttpResponse, HttpRequest, HttpResponse] {
      override def apply(
        request: HttpRequest,
        service: Service[HttpRequest, HttpResponse]): Future[HttpResponse] = {
        service(request).rescue[HttpResponse] {
          case NoResponseException =>
            okResponse
        }
      }
    }
}

trait HttpDifferenceProxy extends DifferenceProxy {
  val servicePort: SocketAddress
  val lifter = new HttpLifter(settings.excludeHttpHeadersComparison)

  override type Req = HttpRequest
  override type Rep = HttpResponse
  override type Srv = HttpService

  override def serviceFactory(serverset: String, label: String) =
    HttpService(Http.newClient(serverset, label).toService)

  override lazy val server =
    Http.serve(
      servicePort,
      HttpDifferenceProxy.noResponseExceptionFilter andThen proxy)

  override def liftRequest(req: HttpRequest): Future[Message] =
    lifter.liftRequest(req)

  override def liftResponse(resp: Try[HttpResponse]): Future[Message] =
    lifter.liftResponse(resp)
}

object SimpleHttpDifferenceProxy {
  /**
   * Side effects can be dangerous if replayed on production backends. This
   * filter ignores all POST, PUT, and DELETE requests if the
   * "allowHttpSideEffects" flag is set to false.
   */
  lazy val httpSideEffectsFilter =
    Filter.mk[HttpRequest, HttpResponse, HttpRequest, HttpResponse] { (req, svc) =>
      val hasSideEffects =
        Set(Method.Post, Method.Put, Method.Delete).contains(Request(req).method)

      if (hasSideEffects) DifferenceProxy.NoResponseExceptionFuture else svc(req)
    }

  val log = Logger(classOf[SimpleHttpDifferenceProxy])
  val NoResponseExceptionFuture = Future.exception(NoResponseException)
}

class ResponseData {
  var primaryMessage: Message = null
  var secondaryMessage: Message = null
  var candidateMessage: Message = null
}

/**
 * A Twitter-specific difference proxy that adds custom filters to unpickle
 * TapCompare traffic from TFE and optionally drop requests that have side
 * effects
 * @param settings    The settings needed by DifferenceProxy
 */
case class SimpleHttpDifferenceProxy(
  settings: Settings,
  collector: InMemoryDifferenceCollector,
  joinedDifferences: JoinedDifferences,
  analyzer: DifferenceAnalyzer)
    extends HttpDifferenceProxy {
  import SimpleHttpDifferenceProxy._

  //private[this] lazy val multicastHandler =
  // new SequentialMulticastService(Seq(primary.client, candidate.client, secondary.client))

  private[this] lazy val multicastHandlerPrimary =
    new SequentialMulticastService(Seq(primary.client))
  private[this] lazy val multicastHandlerSecondary =
    new SequentialMulticastService(Seq(secondary.client))
  private[this] lazy val multicastHandlerCandidate =
    new SequentialMulticastService(Seq(candidate.client))

  private var primaryCount = 0
  private var secondaryCount = 0
  private var candidateCount = 0

  private val primaryResponses = new ListBuffer[Future[Seq[Message]]]()
  private val secondaryResponse = new ListBuffer[Future[Seq[Message]]]()
  private val candidateResponse = new ListBuffer[Future[Seq[Message]]]()

  private val uniqueReqRespMap = new HashMap[String, ResponseData]()
  private val uniqueReqCountMap = new HashMap[String, Int]()
  override val servicePort = settings.servicePort
  override val proxy =
    Filter.identity andThenIf
      (!settings.allowHttpSideEffects, httpSideEffectsFilter) andThen
      customProxy

  def customProxy = new Service[Req, Rep] {
    override def apply(req: Req): Future[Rep] = {
      log.debug("Inside the custom Proxy ")

      val clientType = req.headers().get("client")
      val uniqueReqId = req.headers().get("request-id")
      log.debug("Current uri: " + req.getUri())
      log.debug("Client name : " + req.headers().get("client"))
      log.debug("request-id : " + req.headers().get("request-id"))
      val multicastHandler = clientType match {
        case "primary" => multicastHandlerPrimary
        case "secondary" => multicastHandlerSecondary
        case "candidate" => multicastHandlerCandidate
      }

      val rawResponses =
        multicastHandler(req) respond {
          case Return(_) => log.info("success networking")
          case Throw(t) => log.info(t, "error networking")
        }
      val responses: Future[Seq[Message]] =
        rawResponses flatMap { reps =>
          Future.collect(reps map liftResponse) respond {
            case Return(rs) =>
              log.info(s"success lifting ${rs.head.endpoint}")

            case _ => log.info("error lifting" )
          }
        }
      try {
        if (!uniqueReqRespMap.contains(uniqueReqId)) {
          uniqueReqRespMap.put(uniqueReqId, new ResponseData())
        }

        val responseList = uniqueReqRespMap.get(uniqueReqId).get

        if (clientType == "primary") {
          log.debug("Before getting message ")
          responseList.primaryMessage = Await.result(responses).head
        } else if (clientType == "candidate") {
          log.debug("Before getting message ")
          responseList.candidateMessage = Await.result(responses).head
        } else if (clientType == "secondary") {
          log.debug("Before getting message ")
          responseList.secondaryMessage = Await.result(responses).head
        }

        log.debug("Current Map Size" + uniqueReqRespMap.size)

        if (uniqueReqRespMap.get(uniqueReqId).get.primaryMessage  != null &&
            uniqueReqRespMap.get(uniqueReqId).get.candidateMessage != null && 
            uniqueReqRespMap.get(uniqueReqId).get.secondaryMessage != null) {
          val responseSeq = Seq(uniqueReqRespMap.get(uniqueReqId).get.primaryMessage, uniqueReqRespMap.get(uniqueReqId).get.candidateMessage, uniqueReqRespMap.get(uniqueReqId).get.secondaryMessage)
          log.info("comparing for Response seq:" + uniqueReqId)
          val liftedResponse: Future[Seq[Message]] = Future.value(responseSeq)

          liftedResponse foreach {
            case Seq(primaryResponse, candidateResponse, secondaryResponse) =>
              liftRequest(req) respond {
                case Return(m) =>
                  log.debug(s"success lifting request for ${m.endpoint}")

                case Throw(t) => log.debug(t, "error lifting request")
              } foreach { req =>
                analyzer(req, candidateResponse, primaryResponse, secondaryResponse)
              }
          }

          uniqueReqRespMap.remove(uniqueReqId)
        }
      } catch {
        case unknown => log.info("Caught exception ", unknown)
      }
      Future.value(rawResponses.get().head.get())
    }
  }
}