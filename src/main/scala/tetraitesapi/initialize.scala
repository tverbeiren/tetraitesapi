package tetraitesapi

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import spark.jobserver._
import tetraitesapi.Model.{Farma, Gezo}

import scala.util.Try

/**
  * Input:
  *
  * - path to gezo file, object format
  * - path to farma file, object format
  * - path to files with ATC codes
  */
object initialize extends SparkJob with NamedObjectSupport {

  import Common._

  implicit def rddPersister[T] : NamedObjectPersister[NamedRDD[T]] = new RDDPersister[T]
  implicit def broadcastPersister[U] : NamedObjectPersister[NamedBroadcast[U]] = new BroadcastPersister[U]

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

  override def runJob(sc: SparkContext, config: Config): Any = {

    // The parsed (object file) versions of the data:
    val gezoString:String = Try(config.getString("gezoDb")).getOrElse("/Users/toni/Dropbox/_KUL/LCM/tetraites/tetraitesAPI/src/resources/gezo.txt")
    val farmaString:String = Try(config.getString("farmaDb")).getOrElse("/Users/toni/Dropbox/_KUL/LCM/tetraites/tetraitesAPI/src/resources/farma.txt")
    // The dictionary will be a broadcast variable
    // -- TODO --

    val gezoDb:RDD[Gezo] = loadGezo(sc, gezoString)
    val farmaDb:RDD[Farma] = loadFarma(sc, farmaString)

    namedObjects.update("gezoDb", NamedRDD(gezoDb.cache, forceComputation = false, storageLevel = StorageLevel.NONE))
    namedObjects.update("farmaDb", NamedRDD(farmaDb.cache, forceComputation = false, storageLevel = StorageLevel.NONE))

    // Load dictionary, absolute paths at this moment
    val dict = loadDictionary (sc)
    val dictBroadcast = sc.broadcast(dict)
    namedObjects.update("genes", NamedBroadcast(dictBroadcast))

    Map("metadata" -> "A sample from the data for verification") ++
      Map("dataGezo" -> gezoDb.take(2)) ++
      Map("dataFarma" -> farmaDb.take(2)) ++
      Map("dict" -> dict.take(2))

  }

}
