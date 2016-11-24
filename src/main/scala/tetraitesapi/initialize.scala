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
  * - __gezoDb__: path to gezo file (tab-separated)
  * - __farmaDb__: path to farma file (tab-separated)
  * - __atcDict__: path to files with ATC codes (space-separated, currently only the first 2 are used)
  */
object initialize extends SparkJob with NamedObjectSupport {

  import Common._

  implicit def rddPersister[T] : NamedObjectPersister[NamedRDD[T]] = new RDDPersister[T]
  implicit def broadcastPersister[U] : NamedObjectPersister[NamedBroadcast[U]] = new BroadcastPersister[U]

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

  override def runJob(sc: SparkContext, config: Config): Any = {

    // The parsed (object file) versions of the data:
    val gezoString:String = Try(config.getString("gezoDb")).getOrElse("<Make sure your provide the correct input file>")
    val farmaString:String = Try(config.getString("farmaDb")).getOrElse("<Make sure you provide the correct input file>")
    // The dictionary will be a broadcast variable
    // TODO: Cleanup
    val dictString:String = Try(config.getString("atcDict")).getOrElse("<Make sure you provide the correct input file>")
    val atcDictString = dictString.split(" ").head
    val atcDict7String = dictString.split(" ").tail.head
    // The _nomenclatuur_ codes
    val prestDictString:String = Try(config.getString("prestDict")).getOrElse("<Make sure your provide the correct input file>")

    // Load data from indicated files:w
    val gezoDb:RDD[Gezo] = loadGezo(sc, gezoString)
    val farmaDb:RDD[Farma] = loadFarma(sc, farmaString)

    namedObjects.update("gezoDb", NamedRDD(gezoDb.cache, forceComputation = false, storageLevel = StorageLevel.NONE))
    namedObjects.update("farmaDb", NamedRDD(farmaDb.cache, forceComputation = false, storageLevel = StorageLevel.NONE))

    // Load dictionary, absolute paths at this moment
    val atcDict = loadDictionary(sc, atcDictString, atcDict7String)
    val atcDictBroadcast = sc.broadcast(atcDict)
    namedObjects.update("atcDict", NamedBroadcast(atcDictBroadcast))

    // Load dictionary for prestaties, absolute paths at this moment
    val prestDict = loadPrestDictionary(sc, prestDictString)
    val prestDictBroadcast = sc.broadcast(prestDict)
    namedObjects.update("prestDict", NamedBroadcast(prestDictBroadcast))

    // Convert to histories for Gezo
    val gezoTimeline = (createHistoriesGezo(sc) _ andThen annotateIsHospital _ andThen annotateIsHospitalWindow _)(gezoDb)

    // Convert to histories for Farma
    val farmaTimeline = createHistoriesFarma(farmaDb)

    // Persist the histories as well
    namedObjects.update("gezoTimeline", NamedRDD(gezoTimeline.cache, forceComputation = false, storageLevel = StorageLevel.NONE))
    namedObjects.update("farmaTimeline", NamedRDD(farmaTimeline.cache, forceComputation = false, storageLevel = StorageLevel.NONE))

    Map("metadata" -> "A sample from the data for verification") ++
      Map("dataGezo" -> gezoDb.take(2)) ++
      Map("dataFarma" -> farmaDb.take(2)) ++
      Map("atcDict" -> atcDict.take(2)) ++
      Map("prestDict" -> prestDict.take(10)) ++
      Map("gezoTimeline" -> gezoTimeline.take(2)) ++
      Map("farmaTimeline" -> farmaTimeline.take(2))
  }

}
