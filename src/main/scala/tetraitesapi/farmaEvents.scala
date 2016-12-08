package tetraitesapi

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver._
import tetraitesapi.Model._

import scala.util.Try

object farmaEvents extends SparkJob with NamedObjectSupport {

  import Common._

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

    // Dictionary Names
    val NamedBroadcast(atcDictNamesBc) = namedObjects.get[NamedBroadcast[scala.collection.Map[String,String]]]("atcDictNames").get
    val atcDictNames = atcDictNamesBc.value

    // Dictionary Prestaties
    val NamedBroadcast(prestDictBc) = namedObjects.get[NamedBroadcast[scala.collection.Map[String,String]]]("prestDict").get
    val prestDict = prestDictBc.value


    // Convert to proper format for _output_
    val resultAsModel = farmaDb
      .filter(_.lidano == lidanoQuery)
      .filter(_.baDat == dayQuery)

    val resultAsMap =
      resultAsModel
        .map(_.asMap)
        .collect
        // Add description for prestatie
        .map(m => m ++ Map("prestatieDesc" -> prestDict.getOrElse(m("prestatie"), "")))
        // Add atc code when present
        .map(m => m ++ Map("atc" -> atcDict.getOrElse(m("farmprod"), "")))
        .map(m => m ++ Map("name" -> atcDictNames.getOrElse(m("farmprod"), "")))
        .map(m => m ++ parseAmounts(Some(m("bed")), Some(m("perstus")), Some(m("gev"))))
        .map(m => m - "perstus" - "bed")

    Map("meta" -> s"Events for farma for ${lidanoQuery} on ${dayQuery}") ++
      Map("data" -> resultAsMap)

  }

}
