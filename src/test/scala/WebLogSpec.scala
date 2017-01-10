import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class WebLogSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val fixture =
    "2015-07-22T09:03:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2\n2015-07-22T09:04:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2\n2015-07-22T11:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2\n2015-07-22T11:01:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2\n2015-07-22T09:01:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2\n2015-07-22T11:02:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2\n2015-07-22T11:03:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2\n2015-07-22T11:04:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2\n2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2\n2015-07-22T09:02:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2\n2015-07-22T09:02:28.019143Z marketpalce-shop 12.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"
  val lines = fixture.split('\n')

  val conf = new SparkConf().setMaster("local").setAppName("WebLogChallenge")

  val sc = new SparkContext(conf)

  override def afterAll {
    if (sc != null) {
      sc.stop()
    }
  }

  def getSessionizedData: RDD[(String, List[List[LogEvent]])] = {
    val interval = 15 * 60
    val cleanedData = WebLogChallenge.cleanData(sc.parallelize(lines))
    WebLogChallenge.sessionizeData(interval, cleanedData)
  }

  "clean data" should "produce the expected rdd" in {
    def isSorted(l: List[LogEvent]) =
      l.view.zip(l.tail).forall(x => x._1.time <= x._2.time)

    val cleanedData = WebLogChallenge.cleanData(sc.parallelize(lines))

    cleanedData.count() should be(2)
    cleanedData.map(_._2).collect().forall(isSorted) should be(true)
    cleanedData
      .map(_._2.size)
      .collect()
      .forall(x => x == 1 || x == 10) should be(true)
  }

  "sessionizeData" should "create 2 sessions for 1 user, and 1 for the other" in {
    val sessionizedData = getSessionizedData

    sessionizedData.count() should be(2)
    sessionizedData
      .map(_._2)
      .collect()
      .forall(sessions =>
        if (sessions.size > 1) sessions.forall(_.size == 5)
        else sessions.head.size == 1) should be(true)
  }

  "getSessionDurationByIP" should "get 0 for 1 user, and 240 for all of the other users sessions" in {
    val sessionizedData = getSessionizedData
    val sessionDurationByIP =
      WebLogChallenge.getSessionDurationByIP(sessionizedData)

    sessionDurationByIP.count() should be(2)
    sessionDurationByIP
      .map(_._2)
      .collect()
      .forall(sessions =>
        if (sessions.size > 1) sessions.forall(_ == 240)
        else sessions.head == 0) should be(true)
  }

  "getAverageSessionDurationByIP" should "get 0 for 1 user, and 240s for the other" in {
    val sessionizedData = getSessionizedData
    val sessionDurationByIP =
      WebLogChallenge.getSessionDurationByIP(sessionizedData)
    val averageSessionDurationByIP =
      WebLogChallenge.getAverageSessionDurationByIP(sessionDurationByIP)

    averageSessionDurationByIP.count() should be(2)
    averageSessionDurationByIP
      .map(_._2)
      .collect()
      .forall(x => x == 0 || x == 240) should be(true)
  }

  "getUniqueLinksByIP" should "get 1 link for both users for all 3 sessions" in {
    val sessionizedData = getSessionizedData
    val uniqueLinksByIP = WebLogChallenge.getUniqueLinksByIP(sessionizedData)

    uniqueLinksByIP.count() should be(2)
    uniqueLinksByIP.map(_._2).collect().forall(_.forall(_ == 1)) should be(
      true)
  }

  "getMostEngagedUsers" should "return user 123.242.248.130 as the most engaged" in {
    val sessionizedData = getSessionizedData
    val sessionDurationByIP =
      WebLogChallenge.getSessionDurationByIP(sessionizedData)
    val mostEngagedUsers =
      WebLogChallenge.getMostEngagedUsers(1, sessionDurationByIP)

    mostEngagedUsers.length should be(1)
    mostEngagedUsers.head._1 should be("123.242.248.130")
  }
}
