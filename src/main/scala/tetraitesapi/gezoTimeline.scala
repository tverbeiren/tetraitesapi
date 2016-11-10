package tetraitesapi

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import spark.jobserver._
import scala.util.Try

import tetraitesapi.Model._


/**
  */
object gezoTimeline extends SparkJob with NamedObjectSupport {

  import Common._

  implicit def rddPersister[T] : NamedObjectPersister[NamedRDD[T]] = new RDDPersister[T]
  implicit def broadcastPersister[U] : NamedObjectPersister[NamedBroadcast[U]] = new BroadcastPersister[U]

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

  override def runJob(sc: SparkContext, config: Config): Any = {

    // Fetch raw events
    val NamedRDD(gezoDb, _ ,_) = namedObjects.get[NamedRDD[Gezo]]("gezoDb").get
    val NamedRDD(farmaDb, _ ,_) = namedObjects.get[NamedRDD[Farma]]("farmaDb").get

    // Fetch timeline
    val NamedRDD(gezoTimeline, _ ,_) = namedObjects.get[NamedRDD[TimelineGezo]]("gezoTimeline").get
    val NamedRDD(farmaTimeline, _ ,_) = namedObjects.get[NamedRDD[TimelineFarma]]("farmaTimeline").get

    // Convert to proper format for _output_
    val resultAsMap = gezoTimeline
      .collect
      .map{case TimelineGezo(key, events, meta) =>
          Map("lidano" -> key.lidano, "date" -> key.baDat, "meta" -> meta)}

    Map("meta" -> "Timeline for gezo") ++
    Map("data" -> resultAsMap)

  }

}
