package tetraitesapi

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
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
object recommend extends SparkJob with NamedObjectSupport {

  implicit def rddPersister[T] : NamedObjectPersister[NamedRDD[T]] = new RDDPersister[T]
  implicit def broadcastPersister[U] : NamedObjectPersister[NamedBroadcast[U]] = new BroadcastPersister[U]

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

  override def runJob(sc: SparkContext, config: Config): Any = {

    val lidanoQuery:String = Try(config.getString("lidano")).getOrElse("no lidano specified")

    // Fetch timeline
    val NamedRDD(gezoTimeline, _ ,_) = namedObjects.get[NamedRDD[TimelineGezo]]("gezoTimeline").get
    val NamedRDD(farmaTimeline, _ ,_) = namedObjects.get[NamedRDD[TimelineFarma]]("farmaTimeline").get

    // Dictionary ATC codes
    val NamedBroadcast(atcDictBc) = namedObjects.get[NamedBroadcast[scala.collection.Map[String,String]]]("atcDict").get
    val atcDict = atcDictBc.value

    // Dictionary Prestaties
    val NamedBroadcast(prestDictBc) = namedObjects.get[NamedBroadcast[scala.collection.Map[String,String]]]("prestDict").get
    val prestDict = prestDictBc.value


    // Define filters and apply to lists of drugs aggregated over years
    def yearAggregation(r: TimelineFarma):String = r match {
      case TimelineFarma(key, _, _) => key.baDat.take(4)
    }

    case class AtcFilter(id: String, f: List[String] => Boolean, comment:String) extends Serializable {
      def run = f
    }

    // Hard-coded definition of the filters to be applied, later to be loaded from file
    val testFilter = AtcFilter("2", list => list.contains("N02BA01"), "Test filter with known ATC code N02BA01")

    // https://www.sfk.nl/documentatie/kissmatrix/kiss-matrix-indicatoren%202013#Min_3a
    val filter1 = AtcFilter("Min_3a Co-trimoxazol met coumarines",
                            list => (list.contains("J01EE01") && (list.contains("B01AA04") || list.contains("B01AA07"))),
                            "Effect van coumarines wordt versterkt met risico op bloeding")

    // List of filters:
    val filters = List(testFilter, filter1)

    // Make the rest of the code readable, using these types:
    type Year = String
    type Lidano = String
    type Combinations = List[String]

    // Convert to proper format for _output_
    val cnksPerYear:RDD[(Year, Lidano, Combinations)] =
      farmaTimeline
        // Remove the following line and the rest can be applied to the full dataset!
        .filter(_.key.lidano.matches(lidanoQuery))
        .groupBy(x => (x.key.lidano, yearAggregation(x)))
        .map{case (keys, buffer) => (keys._2, buffer.head.key.lidano, buffer.flatMap(_.events).map(_.farmprod.getOrElse("NA")).toList)}

    val atcsPerYear:RDD[(Year, Lidano, Combinations)] =
      cnksPerYear
        .map{case (year, lidano, cnkList) => (year, lidano, cnkList.map(l => atcDict.getOrElse(l, "NA")))}

    val parsedAtcsPerYear = //:RDD[(Year, Lidano, Combinations, List[AtcFilter])] =
      atcsPerYear
        // Run the filters defined above, keep the once that evaluate to true
        .map{case (year, lidano, atcList) =>
          (year, lidano, atcList, filters.filter(f => f.run(atcList)))
        }
        // Filter out the records with no matching filters
        .filter{case (year, lidano, atcList, matchingFilters) => atcList.nonEmpty }
        // extract the filter comment for output
        .map{case (year, lidano, atcList, matchingFilters) =>
          (year, lidano, matchingFilters.map(_.comment))
        }
        .collect
        // Sort by year for convencience of interpretation
        .sortBy(_._1)
        // Convert to Key-Value pairs
        .map{case (year, lidano, comments) =>
          Map(
            "year" -> year,
            "lidano" -> lidano,
            "comments" -> comments
          )
      }

    parsedAtcsPerYear


  }

}
