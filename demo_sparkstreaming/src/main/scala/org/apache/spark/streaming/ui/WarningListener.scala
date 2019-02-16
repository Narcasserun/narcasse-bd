package org.apache.spark.streaming.ui

import java.util.{LinkedHashMap, Properties, Map => JMap}

import org.apache.spark.streaming.scheduler.{StreamingListenerBatchStarted, StreamingListenerBatchSubmitted, StreamingListenerReceiverStarted, _}
import org.apache.spark.streaming.{StreamingContext, Time}

import scala.collection.mutable._

/**
 * Created by spark on 2019/1/8.
 */
class WarningListener(ssc: StreamingContext) extends StreamingListener  {
  private val waitingBatchUIData = new HashMap[Time, BatchUIData]
  private val runningBatchUIData = new HashMap[Time, BatchUIData]
  private val completedBatchUIData = new Queue[BatchUIData]
  private val batchUIDataLimit = ssc.conf.getInt("spark.streaming.ui.retainedBatches", 1000)
  private var totalCompletedBatches = 0L
  private var totalReceivedRecords = 0L
  private var totalProcessedRecords = 0L
  private val receiverInfos = new HashMap[Int, ReceiverInfo]

  // Because onJobStart and onBatchXXX messages are processed in different threads,
  // we may not be able to get the corresponding BatchUIData when receiving onJobStart. So here we
  // cannot use a map of (Time, BatchUIData).
  private[ui] val batchTimeToOutputOpIdSparkJobIdPair =
    new LinkedHashMap[Time, SynchronizedBuffer[OutputOpIdAndSparkJobId]] {
      override def removeEldestEntry(
                                      p1: JMap.Entry[Time, SynchronizedBuffer[OutputOpIdAndSparkJobId]]): Boolean = {
        // If a lot of "onBatchCompleted"s happen before "onJobStart" (image if
        // SparkContext.listenerBus is very slow), "batchTimeToOutputOpIdToSparkJobIds"
        // may add some information for a removed batch when processing "onJobStart". It will be a
        // memory leak.
        //
        // To avoid the memory leak, we control the size of "batchTimeToOutputOpIdToSparkJobIds" and
        // evict the eldest one.
        //
        // Note: if "onJobStart" happens before "onBatchSubmitted", the size of
        // "batchTimeToOutputOpIdToSparkJobIds" may be greater than the number of the retained
        // batches temporarily, so here we use "10" to handle such case. This is not a perfect
        // solution, but at least it can handle most of cases.
        size() >
          waitingBatchUIData.size + runningBatchUIData.size + completedBatchUIData.size + 10
      }
    }


  val batchDuration = ssc.graph.batchDuration.milliseconds

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) {
    synchronized {
      receiverInfos(receiverStarted.receiverInfo.streamId) = receiverStarted.receiverInfo
    }
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) {
    synchronized {
      receiverInfos(receiverError.receiverInfo.streamId) = receiverError.receiverInfo
    }
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) {
    synchronized {
      receiverInfos(receiverStopped.receiverInfo.streamId) = receiverStopped.receiverInfo
    }
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    synchronized {
      waitingBatchUIData(batchSubmitted.batchInfo.batchTime) =
        BatchUIData(batchSubmitted.batchInfo)
    }
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = synchronized {
    val batchUIData = BatchUIData(batchStarted.batchInfo)
    runningBatchUIData(batchStarted.batchInfo.batchTime) = BatchUIData(batchStarted.batchInfo)
    waitingBatchUIData.remove(batchStarted.batchInfo.batchTime)

    totalReceivedRecords += batchUIData.numRecords
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    synchronized {
      waitingBatchUIData.remove(batchCompleted.batchInfo.batchTime)
      runningBatchUIData.remove(batchCompleted.batchInfo.batchTime)
      val batchUIData = BatchUIData(batchCompleted.batchInfo)
      completedBatchUIData.enqueue(batchUIData)
      if (completedBatchUIData.size > batchUIDataLimit) {
        val removedBatch = completedBatchUIData.dequeue()
        batchTimeToOutputOpIdSparkJobIdPair.remove(removedBatch.batchTime)
      }
      totalCompletedBatches += 1L

      totalProcessedRecords += batchUIData.numRecords
    }
  }

  override def onOutputOperationStarted(
                                         outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = synchronized {
    // This method is called after onBatchStarted
    runningBatchUIData(outputOperationStarted.outputOperationInfo.batchTime).
      updateOutputOperationInfo(outputOperationStarted.outputOperationInfo)
  }

  override def onOutputOperationCompleted(
                                           outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = synchronized {
    // This method is called before onBatchCompleted
    runningBatchUIData(outputOperationCompleted.outputOperationInfo.batchTime).
      updateOutputOperationInfo(outputOperationCompleted.outputOperationInfo)
  }


  private def getBatchTimeAndOutputOpId(properties: Properties): Option[(Time, Int)] = {
    val batchTime = properties.getProperty(JobScheduler.BATCH_TIME_PROPERTY_KEY)
    if (batchTime == null) {
      // Not submitted from JobScheduler
      None
    } else {
      val outputOpId = properties.getProperty(JobScheduler.OUTPUT_OP_ID_PROPERTY_KEY)
      assert(outputOpId != null)
      Some(Time(batchTime.toLong) -> outputOpId.toInt)
    }
  }

  def numReceivers: Int = synchronized {
    receiverInfos.size
  }

  def numActiveReceivers: Int = synchronized {
    receiverInfos.count(_._2.active)
  }

  def numInactiveReceivers: Int = {
    ssc.graph.getReceiverInputStreams().size - numActiveReceivers
  }

  def numTotalCompletedBatches: Long = synchronized {
    totalCompletedBatches
  }

  def numTotalReceivedRecords: Long = synchronized {
    totalReceivedRecords
  }

  def numTotalProcessedRecords: Long = synchronized {
    totalProcessedRecords
  }

  def numUnprocessedBatches: Long = synchronized {
    waitingBatchUIData.size + runningBatchUIData.size
  }

//  def waitingBatches: Seq[BatchUIData] = synchronized {
//    waitingBatchUIData.values.toSeq
//  }
//
//  def runningBatches: Seq[BatchUIData] = synchronized {
//    runningBatchUIData.values.toSeq
//  }

//  def retainedCompletedBatches: Seq[BatchUIData] = synchronized {
//    completedBatchUIData.toSeq
//  }

//  def streamName(streamId: Int): Option[String] = {
//    ssc.graph.getInputStreamName(streamId)
//  }

  /**
   * Return all InputDStream Ids
   */
  def streamIds: Seq[Int] = ssc.graph.getInputStreams().map(_.id)

  /**
   * Return all of the event rates for each InputDStream in each batch. The key of the return value
   * is the stream id, and the value is a sequence of batch time with its event rate.
   */
//  def receivedEventRateWithBatchTime: Map[Int, Seq[(Long, Double)]] = synchronized {
//    val _retainedBatches = retainedBatches
//    val latestBatches = _retainedBatches.map { batchUIData =>
//      (batchUIData.batchTime.milliseconds, batchUIData.streamIdToInputInfo.mapValues(_.numRecords))
//    }
//    streamIds.map { streamId =>
//      val eventRates = latestBatches.map {
//        case (batchTime, streamIdToNumRecords) =>
//          val numRecords = streamIdToNumRecords.getOrElse(streamId, 0L)
//          (batchTime, numRecords * 1000.0 / batchDuration)
//      }
//      (streamId, eventRates)
//    }.toMap
//  }

//  def lastReceivedBatchRecords: Map[Int, Long] = synchronized {
//    val lastReceivedBlockInfoOption =
//      lastReceivedBatch.map(_.streamIdToInputInfo.mapValues(_.numRecords))
//    lastReceivedBlockInfoOption.map { lastReceivedBlockInfo =>
//      streamIds.map { streamId =>
//        (streamId, lastReceivedBlockInfo.getOrElse(streamId, 0L))
//      }.toMap
//    }.getOrElse {
//      streamIds.map(streamId => (streamId, 0L)).toMap
//    }
//  }

  def receiverInfo(receiverId: Int): Option[ReceiverInfo] = synchronized {
    receiverInfos.get(receiverId)
  }

  def lastCompletedBatch: Option[BatchUIData] = synchronized {
    completedBatchUIData.sortBy(_.batchTime)(Time.ordering).lastOption
  }

//  def lastReceivedBatch: Option[BatchUIData] = synchronized {
//    retainedBatches.lastOption
//  }

//  def retainedBatches: Seq[BatchUIData] = synchronized {
//    (waitingBatchUIData.values.toSeq ++
//      runningBatchUIData.values.toSeq ++ completedBatchUIData).sortBy(_.batchTime)(Time.ordering)
//  }

//  def getBatchUIData(batchTime: Time): Option[BatchUIData] = synchronized {
//    val batchUIData = waitingBatchUIData.get(batchTime).orElse {
//      runningBatchUIData.get(batchTime).orElse {
//        completedBatchUIData.find(batch => batch.batchTime == batchTime)
//      }
//    }
//    batchUIData.foreach { _batchUIData =>
//      val outputOpIdToSparkJobIds =
//        Option(batchTimeToOutputOpIdSparkJobIdPair.get(batchTime)).getOrElse(Seq.empty)
//      _batchUIData.outputOpIdSparkJobIdPairs = outputOpIdToSparkJobIds
//    }
//    batchUIData
//  }
}
object WarningListener {
  type SparkJobId = Int
  type OutputOpId = Int
}