package tetraitesapi

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver._
import tetraitesapi.Model._

import scala.util.Try


/**
  * Query the timeline for one of more lidano's for a certain window of time.
  *
  * Input:
  *
  * - __lidano__: A regular expression for the lidano key (default: `.*`)
  * - __start__: the start date of a window of interest (default: 1/1/1900)
  * - __end__: The end date of a window of interest (default: 1/1/2500)
  * - __window__: Specify a window using regular expressions on the string dates (default: `.*`)
  */
object farmaTimeline extends SparkJob with NamedObjectSupport {

  import Common._

  implicit def rddPersister[T] : NamedObjectPersister[NamedRDD[T]] = new RDDPersister[T]
  implicit def broadcastPersister[U] : NamedObjectPersister[NamedBroadcast[U]] = new BroadcastPersister[U]

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

  override def runJob(sc: SparkContext, config: Config): Any = {

    val lidanoQuery:String = Try(config.getString("lidano")).getOrElse(".*")
    val windowStart:String = Try(config.getString("start")).getOrElse("19000101")
    val windowEnd:String = Try(config.getString("end")).getOrElse("25000101")
    val window:String = Try(config.getString("window")).getOrElse(".*")

    // The dictionary will be a broadcast variable
    // -- TODO --

    // Fetch raw events
    val NamedRDD(gezoDb, _ ,_) = namedObjects.get[NamedRDD[Gezo]]("gezoDb").get
    val NamedRDD(farmaDb, _ ,_) = namedObjects.get[NamedRDD[Farma]]("farmaDb").get

    // Fetch timeline
    val NamedRDD(gezoTimeline, _ ,_) = namedObjects.get[NamedRDD[TimelineGezo]]("gezoTimeline").get
    val NamedRDD(farmaTimeline, _ ,_) = namedObjects.get[NamedRDD[TimelineFarma]]("farmaTimeline").get

    // Convert to proper format for _output_
    val resultAsMap = farmaTimeline
      .filter(_.key.lidano.matches(lidanoQuery))
      .filter(_.key.baDat >= windowStart)
      .filter(_.key.baDat <= windowEnd)
      .filter(_.key.baDat.matches(window))
      .collect
      .map{case TimelineFarma(key, events, meta) =>
          Map("lidano" -> key.lidano,
              "date" -> key.baDat,
              "meta" -> meta) ++ sumAmountsFarma(events.toArray)
      }

    Map("meta" -> "Timeline for farma") ++
    Map("data" -> resultAsMap)

  }

}
