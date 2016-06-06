package com.intel.hibench.streambench.gearpump.metrics

import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.pattern.ask
import java.util.{TimerTask, Timer}

import com.intel.hibench.streambench.common.Logger
import org.apache.gearpump.cluster.ClientToMaster.{ReadOption, QueryHistoryMetrics}
import org.apache.gearpump.cluster.MasterToAppMaster.AppMasterDataDetailRequest
import org.apache.gearpump.cluster.MasterToClient.HistoryMetrics
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.metrics.Metrics.Histogram
import org.apache.gearpump.streaming.appmaster.StreamAppMasterSummary

import scala.concurrent.{Future, Await}
import scala.concurrent.duration.Duration
import scala.util.Try

class MetricsReporter(appId: Int, clientContext: ClientContext, logger: Logger, recordCount: Long) {
  implicit val timeOut = akka.util.Timeout(15, TimeUnit.SECONDS)
  private val startTime = System.currentTimeMillis()
  private val mutex = new Object
  private val INTERVAL = 1000 // 1 second
  private var finished = false
  private var finishTime = 0L
  private var averageLatency: Double = -1

  private def waitToFinish() = {
    val timer = new Timer
    timer.schedule(new CheckFinish(timer), 0, INTERVAL)
    mutex.synchronized {
      mutex.wait()
    }
    finished = true
  }

  def reportMetrics(): Unit = {
    if (!finished) {
      waitToFinish()
    }
    val runTime = (finishTime - startTime) / 1000
    logger.logThroughput((recordCount / runTime).toString)
    logger.logAvgLatency(averageLatency.toString)
  }

  class CheckFinish(timer: Timer) extends TimerTask {
    private var appMaster: ActorRef = null
    private val queryMetrics = QueryHistoryMetrics(s"app$appId", ReadOption.ReadLatest)

    override def run(): Unit = {
      if (appMaster != null) {
        val summaryFuture = (appMaster ? AppMasterDataDetailRequest(appId)).asInstanceOf[Future[StreamAppMasterSummary]]
        val summary = Await.result(summaryFuture, Duration(60, TimeUnit.SECONDS))
        // All the messages are consumed
        if (summary.clock == Long.MaxValue) {
          finishTime = System.currentTimeMillis()
          val metricsFuture = (appMaster ? queryMetrics).asInstanceOf[Future[HistoryMetrics]]
          val metrics = Await.result(metricsFuture, Duration(60, TimeUnit.SECONDS))
          logLatency(metrics)
          mutex.synchronized {
            mutex.notify()
          }
          timer.cancel
          timer.purge
        }
      } else {
        Try(clientContext.resolveAppID(appId)).map(result => appMaster = result)
      }
    }

    private def logLatency(metrics: HistoryMetrics): Unit = {
      val latencyMetrics = metrics.metrics.filter(_.value.isInstanceOf[Histogram])
      averageLatency = latencyMetrics.foldLeft(0.0) { (total, metric) =>
        val histogram = metric.value.asInstanceOf[Histogram]
        if (histogram.name.endsWith("receiveLatency") || histogram.name.endsWith("processTime")) {
          total + histogram.mean
        } else {
          total
        }
      }
    }
  }

}
