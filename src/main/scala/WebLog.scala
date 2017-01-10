import java.time.Instant

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class LogEvent(time: Long, url: String)

object WebLogChallenge {
  def cleanData(rdd: RDD[String]): RDD[(String, List[LogEvent])] = {
    rdd
      .map(_.split(' '))
      .groupBy(_(2).split(':').head) // group by the ip
      .mapValues(_.map(x =>
        LogEvent(Instant.parse(x(0)).getEpochSecond, x(12)) // event time and url
      ).toList.sortBy(_.time))
  }

  // (1) sessionized data based on a fixed time window
  def sessionizeData(interval: Int, rdd: RDD[(String, List[LogEvent])])
    : RDD[(String, List[List[LogEvent]])] = {
    rdd
      .mapValues(events => {
        if (events.length == 1) List(List(events.head))
        else {
          val zeroAcc =
            (List.empty[List[LogEvent]], List(events.head), events.head)
          val (groups, lastSession, _) = events.tail.foldLeft(zeroAcc)(
            (acc, currentEvent) =>
              if (math.abs(acc._3.time - currentEvent.time) <= interval)
                (acc._1, acc._2 :+ currentEvent, currentEvent)
              else (acc._1 :+ acc._2, List(currentEvent), currentEvent)
          )
          groups :+ lastSession
        }
      })
      .cache() // cache here since this rdd is the basis of all the queries.
  }

  // session durations for each IP
  def getSessionDurationByIP(
      rdd: RDD[(String, List[List[LogEvent]])]): RDD[(String, List[Long])] = {
    rdd.mapValues(_.map(x =>
      if (x.size <= 1) 0 else x.maxBy(_.time).time - x.minBy(_.time).time))
  }

  // (2) average session duration per IP
  def getAverageSessionDurationByIP(
      rdd: RDD[(String, List[Long])]): RDD[(String, Long)] = {
    rdd.mapValues(x => x.sum / x.size)
  }

  // (3) unique link views per IP per session
  def getUniqueLinksByIP(
      rdd: RDD[(String, List[List[LogEvent]])]): RDD[(String, List[Int])] = {
    rdd.mapValues(allSessionsForUser =>
      allSessionsForUser.map(session => session.map(_.url).distinct.size))
  }

  // (4) most engaged
  def getMostEngagedUsers(
      n: Int,
      rdd: RDD[(String, List[Long])]): Array[(String, Long)] =
    rdd.mapValues(_.max).sortBy(-_._2).take(n)

  def main(args: Array[String]): Unit = {
    val interval = 15 * 60

    val conf = new SparkConf().setAppName("WebLogChallenge")
    val sc = new SparkContext(conf)

    val file: RDD[String] =
      if (args.nonEmpty) sc.textFile(args(0)) else sc.emptyRDD[String]

    val sessionizedDataByIP: RDD[(String, List[List[LogEvent]])] =
      sessionizeData(interval, cleanData(file))
    val sessionsDurationsByIP = getSessionDurationByIP(sessionizedDataByIP)

    // 2
    val averageSessionDurationByIP = getAverageSessionDurationByIP(
      sessionsDurationsByIP)
    // 3
    val uniqueLinksByIP = getUniqueLinksByIP(sessionizedDataByIP)
    // 4
    val mostEngagedIP = getMostEngagedUsers(10, sessionsDurationsByIP)

    sc.stop()
  }
}
