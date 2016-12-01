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
object interpret extends SparkJob with NamedObjectSupport {

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

    // Convert to proper format for _output_
    val filteredGezoDb = gezoDb
      .filter(_.lidano == lidanoQuery)
      .filter(_.baDat == dayQuery)

    // Convert to proper format for _output_
    val filteredFarmaDb = farmaDb
      .filter(_.lidano == lidanoQuery)
      .filter(_.baDat == dayQuery)

    /**
      * Very basic prototype of how we can parse records to extract some interpretation.
      *
      * In this approach, only _local_ information is used. This can be improved upon.
      *
      */
    def interpretFarma(record:Farma):Option[String] = {
      record match {
        case Farma(_, _, Some("2642619"), _, _, _, _, _, _, _, _, _, _, _, _, _, _) => Some("ibuprofen")
        case _ => None
      }
    }

    /**
      * Very basic prototype of how we can parse records to extract some interpretation.
      *
      * In this approach, only _local_ information is used. This can be improved upon.
      *
      */
    def interpretGezo(record:Gezo):Option[String] = {
      record match {
        case Gezo(_, _, "424104", _, _, _, _, _, _, _, _, _, _, _, _) => Some("bevalling met keizersnede")
        case Gezo(_, _, "301011", _, _, _, _, _, _, _, _, _, _, _, _) => Some("tandarts bezoek")
        case _ => None
      }
    }


    // Very simple aggregation of results
    val interpretationsFarma = filteredFarmaDb.map(interpretFarma).flatMap(x=>x).collect.toList.distinct
    val interpretationsGezo = filteredGezoDb.map(interpretGezo).flatMap(x=>x).collect.toList.distinct

    Map("meta" -> s"Interpretation for $lidanoQuery on $dayQuery") ++
    Map("data" -> (interpretationsGezo ++ interpretationsFarma))

  }

}
