package mesosphere.marathon.core.launchqueue

import mesosphere.marathon.core.launchqueue.LaunchQueue.QueuedTaskCount
import mesosphere.marathon.state.{ AppDefinition, PathId }

import scala.collection.immutable.Seq
import scala.concurrent.duration.Deadline

object LaunchQueue {

  /**
    * @param app the currently used app definition
    * @param tasksLeftToLaunch the tasks that still have to be launched
    * @param taskLaunchesInFlight the number of tasks which have been requested to be launched
    *                             but are unconfirmed yet
    * @param tasksLaunchedOrRunning the number of tasks which are running or at least have been confirmed to be
    *                               launched
    */
  protected[marathon] case class QueuedTaskCount(
    app: AppDefinition,
    tasksLeftToLaunch: Int,
    taskLaunchesInFlight: Int,
    tasksLaunchedOrRunning: Int)
}

/**
  * Utility class to stage tasks before they get scheduled
  */
trait LaunchQueue {

  def list: Seq[QueuedTaskCount]
  def listWithDelay: Seq[(QueuedTaskCount, Deadline)]

  def listApps: Seq[AppDefinition]

  def add(app: AppDefinition, count: Int = 1): Unit
  def count(appId: PathId): Int
  def purge(appId: PathId): Unit

  def resetDelay(appId: PathId): Unit
}
