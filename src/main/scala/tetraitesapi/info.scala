package tetraitesapi

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver._
import tetraitesapi.Model._

import scala.util.Try

object info extends SparkJob with NamedObjectSupport {

  implicit def rddPersister[T] : NamedObjectPersister[NamedRDD[T]] = new RDDPersister[T]
  implicit def broadcastPersister[U] : NamedObjectPersister[NamedBroadcast[U]] = new BroadcastPersister[U]

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

  override def runJob(sc: SparkContext, config: Config): Any = {

    val lidanoQuery:String = Try(config.getString("lidano")).getOrElse("no lidano specified")
    val dayQuery:String = Try(config.getString("day")).getOrElse("no day specified")

    // Fetch raw events
    val NamedRDD(gezoDb, _ ,_) = namedObjects.get[NamedRDD[Gezo]]("gezoDb").get
    val NamedRDD(farmaDb, _ ,_) = namedObjects.get[NamedRDD[Farma]]("farmaDb").get

    // Dictionary ATC codes
    val NamedBroadcast(atcDictBc) = namedObjects.get[NamedBroadcast[scala.collection.Map[String,String]]]("atcDict").get
    val atcDict = atcDictBc.value

    // Dictionary Prestaties
    val NamedBroadcast(prestDictBc) = namedObjects.get[NamedBroadcast[scala.collection.Map[String,String]]]("prestDict").get
    val prestDict = prestDictBc.value

    // Stats
    val nrGezo = gezoDb.count
    val nrFarma = farmaDb.count
    val listLidano = (gezoDb.map(_.lidano).collect.toList ++ farmaDb.map(_.lidano).collect.toList).distinct
    val nrLidano = listLidano.length

    val resultAsMap =
      Map(
        "# persons" -> nrLidano,
        "persons" -> listLidano,
        "nrGezo" -> nrGezo,
        "nrFarma" -> nrFarma
      )

    Map("meta" -> s"General information about the database") ++
      Map("data" -> resultAsMap)

  }

}
