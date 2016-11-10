package tetraitesapi

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver._
import tetraitesapi.Model._

import scala.util.Try


/**
  */
object farmaTimeline extends SparkJob with NamedObjectSupport {

  implicit def rddPersister[T] : NamedObjectPersister[NamedRDD[T]] = new RDDPersister[T]
  implicit def broadcastPersister[U] : NamedObjectPersister[NamedBroadcast[U]] = new BroadcastPersister[U]

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

  override def runJob(sc: SparkContext, config: Config): Any = {

    // The parsed (object file) versions of the data:
    val gezoString:String = Try(config.getString("gezoDb")).getOrElse("/Users/toni/Dropbox/_KUL/LCM/tetraites/tetraitesAPI/src/resources/gezo.txt")
    val farmaString:String = Try(config.getString("farmaDb")).getOrElse("/Users/toni/Dropbox/_KUL/LCM/tetraites/tetraitesAPI/src/resources/farma.txt")
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
      .collect
      .map{case TimelineFarma(key, events, meta) =>
          Map("lidano" -> key.lidano, "date" -> key.baDat, "meta" -> meta)}

    Map("meta" -> "Timeline for farma") ++
    Map("data" -> resultAsMap)

  }

}
