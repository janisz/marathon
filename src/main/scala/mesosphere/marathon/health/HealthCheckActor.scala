package mesosphere.marathon.health

import akka.actor.{ Actor, ActorLogging, ActorRef, Cancellable, Props }
import akka.event.EventStream
import mesosphere.marathon.Protos.HealthCheckDefinition.Protocol
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.bus.MesosTaskStatus.TemporarilyUnreachable
import mesosphere.marathon.core.task.tracker.TaskTracker
import mesosphere.marathon.event._
import mesosphere.marathon.state.{ AppDefinition, Timestamp }
import mesosphere.marathon.MarathonSchedulerDriverHolder
import net.logstash.logback.argument.StructuredArguments.value

private[health] class HealthCheckActor(
    app: AppDefinition,
    marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
    healthCheck: HealthCheck,
    taskTracker: TaskTracker,
    eventBus: EventStream) extends Actor with ActorLogging {

  import context.dispatcher
  import mesosphere.marathon.health.HealthCheckActor.{ GetTaskHealth, _ }
  import mesosphere.marathon.health.HealthCheckWorker.HealthCheckJob

  var nextScheduledCheck: Option[Cancellable] = None
  var taskHealth = Map[Task.Id, Health]()

  val workerProps = Props[HealthCheckWorkerActor]

  override def preStart(): Unit = {
    log.info(
      "Starting health check actor for app [{}] version [{}] and healthCheck [{}]",
      value("appId", app.id),
      value("version", app.version),
      value("healthCheck", healthCheck)
    )
    scheduleNextHealthCheck()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit =
    log.info(
      "Restarting health check actor for app [{}] version [{}] and healthCheck [{}]",
      value("appId", app.id),
      value("version", app.version),
      value("healthCheck", healthCheck)
    )

  override def postStop(): Unit = {
    nextScheduledCheck.forall { _.cancel() }
    log.info(
      "Stopped health check actor for app [{}] version [{}] and healthCheck [{}]",
      value("appId", app.id),
      value("version", app.version),
      value("healthCheck", healthCheck)
    )
  }

  def purgeStatusOfDoneTasks(): Unit = {
    log.debug(
      "Purging health status of done tasks for app [{}] version [{}] and healthCheck [{}]",
      value("appId", app.id),
      value("version", app.version),
      value("healthCheck", healthCheck)
    )
    val activeTaskIds = taskTracker.appTasksLaunchedSync(app.id).map(_.taskId).toSet
    // The Map built with filterKeys wraps the original map and contains a reference to activeTaskIds.
    // Therefore we materialize it into a new map.
    taskHealth = taskHealth.filterKeys(activeTaskIds).iterator.toMap
  }

  def scheduleNextHealthCheck(): Unit =
    if (healthCheck.protocol != Protocol.COMMAND) {
      log.debug(
        "Scheduling next health check for app [{}] version [{}] and healthCheck [{}]",
        value("appId", app.id),
        value("version", app.version),
        value("healthCheck", healthCheck)
      )
      nextScheduledCheck = Some(
        context.system.scheduler.scheduleOnce(healthCheck.interval) {
          self ! Tick
        }
      )
    }

  def dispatchJobs(): Unit = {
    import TemporarilyUnreachable.isUnreachable

    log.debug("Dispatching health check jobs to workers")
    taskTracker.appTasksSync(app.id).foreach { task =>
      task.launched.foreach { launched =>
        if (launched.runSpecVersion == app.version && launched.hasStartedRunning && !isUnreachable(task)) {
          log.debug("Dispatching health check job for {}", value("taskId", task.taskId))
          val worker: ActorRef = context.actorOf(workerProps)
          worker ! HealthCheckJob(app, task, launched, healthCheck)
        }
      }
    }
  }

  def checkConsecutiveFailures(task: Task, health: Health): Unit = {
    val consecutiveFailures = health.consecutiveFailures
    val maxFailures = healthCheck.maxConsecutiveFailures

    // ignore failures if maxFailures == 0
    if (consecutiveFailures >= maxFailures && maxFailures > 0) {
      log.info(
        "Detected unhealthy {} of app {} version [{}] on host {}",
        value("taskId", task.taskId),
        value("appId", app.id),
        value("version", app.version),
        value("host", task.agentInfo.host)
      )

      // kill the task, if it is reachable
      task match {
        case TemporarilyUnreachable(_) =>
          log.warning(
            "Task {} on host {} is temporarily unreachable. Performing no kill.",
            value("taskId", task.taskId),
            value("appId", task.runSpecId),
            value("host", task.agentInfo.host)
          )
        case _ =>
          marathonSchedulerDriverHolder.driver.foreach { driver =>
            log.warning(
              "Send kill request for {} on host {} to driver",
              value("taskId", task.taskId),
              value("appId", task.runSpecId),
              value("host", task.agentInfo.host)
            )
            eventBus.publish(
              UnhealthyTaskKillEvent(
                appId = task.runSpecId,
                taskId = task.taskId,
                version = app.version,
                reason = health.lastFailureCause.getOrElse("unknown"),
                host = task.agentInfo.host,
                slaveId = task.agentInfo.agentId,
                timestamp = health.lastFailure.getOrElse(Timestamp.now()).toString
              )
            )
            driver.killTask(task.taskId.mesosTaskId)
          }
      }
    }
  }

  def ignoreFailures(task: Task, health: Health): Boolean = {
    // Ignore failures during the grace period, until the task becomes green
    // for the first time.  Also ignore failures while the task is staging.
    task.launched.fold(true) { launched =>
      health.firstSuccess.isEmpty &&
        launched.status.startedAt.fold(true) { startedAt =>
          startedAt + healthCheck.gracePeriod > Timestamp.now()
        }
    }
  }

  //TODO: fix style issue and enable this scalastyle check
  //scalastyle:off cyclomatic.complexity method.length
  def receive: Receive = {
    case GetTaskHealth(taskId) => sender() ! taskHealth.getOrElse(taskId, Health(taskId))

    case GetAppHealth =>
      sender() ! AppHealth(taskHealth.values.toSeq)

    case Tick =>
      purgeStatusOfDoneTasks()
      dispatchJobs()
      scheduleNextHealthCheck()

    case result: HealthResult if result.version == app.version =>
      log.info(
        "Received health result for app [{}] version [{}]: [{}]",
        value("appId", app.id),
        value("version", app.version),
        value("result", result))
      val taskId = result.taskId
      val health = taskHealth.getOrElse(taskId, Health(taskId))

      val newHealth = result match {
        case Healthy(_, _, _) =>
          health.update(result)
        case Unhealthy(_, _, _, _) =>
          taskTracker.tasksByAppSync.task(taskId) match {
            case Some(task) =>
              if (ignoreFailures(task, health)) {
                // Don't update health
                health
              } else {
                eventBus.publish(FailedHealthCheck(app.id, taskId, healthCheck))
                checkConsecutiveFailures(task, health)
                health.update(result)
              }
            case None =>
              log.error(
                s"Couldn't find task {}",
                value("taskId", taskId))
              health.update(result)
          }
      }

      taskHealth += (taskId -> newHealth)

      if (health.alive != newHealth.alive) {
        eventBus.publish(
          HealthStatusChanged(
            appId = app.id,
            taskId = taskId,
            version = result.version,
            alive = newHealth.alive)
        )
      }

    case result: HealthResult =>
      log.warning(
        s"Ignoring health result [{}] due to version mismatch.",
        value("taskId", result))

  }
}

object HealthCheckActor {
  def props(
    app: AppDefinition,
    marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
    healthCheck: HealthCheck,
    taskTracker: TaskTracker,
    eventBus: EventStream): Props = {

    Props(new HealthCheckActor(
      app,
      marathonSchedulerDriverHolder,
      healthCheck,
      taskTracker,
      eventBus))
  }

  // self-sent every healthCheck.intervalSeconds
  case object Tick
  case class GetTaskHealth(taskId: Task.Id)
  case object GetAppHealth

  case class AppHealth(health: Seq[Health])
}
