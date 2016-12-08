package tetraitesapi

import scala.util.Try
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import Model._

object Common extends Serializable {

  def parseOptionString(os: Option[String]):Option[String] = os match {
    case None => None
    case Some("") => None
    case _ => os
  }

  /**
    * Function to convert case class (Product) to a map.
    * @param cc
    * @return
    */
  def getCCParams(cc: AnyRef) =
  (Map[String, Any]() /: cc.getClass.getDeclaredFields) {(a, f) =>
    f.setAccessible(true)
    a + (f.getName -> f.get(cc))
  }

  def loadFarma(sc: SparkContext, fileName:String, filter:Farma => Boolean = _ => true):RDD[Farma] = {

    val rawFarma = sc.textFile(fileName)

    rawFarma
      .zipWithIndex
      .filter(_._2 > 0)
      .map(_._1)
      .map(_.split("\t"))
      .map(r =>
        Farma(
          r(0),
          r(1),
          parseOptionString(Try(r(2)).toOption),
          parseOptionString(Try(r(3)).toOption),
          r(4),
          parseOptionString(Try(r(5)).toOption),
          parseOptionString(Try(r(6)).toOption),
          parseOptionString(Try(r(7)).toOption),
          parseOptionString(Try(r(8)).toOption),
          parseOptionString(Try(r(9)).toOption),
          parseOptionString(Try(r(10)).toOption),
          parseOptionString(Try(r(11)).toOption),
          parseOptionString(Try(r(12)).toOption),
          parseOptionString(Try(r(13)).toOption),
          parseOptionString(Try(r(14)).toOption),
          parseOptionString(Try(r(15)).toOption),
          parseOptionString(Try(r(16)).toOption)
        )
      )
      .filter(filter)
      .cache
  }

  def loadGezo(sc: SparkContext, fileName:String, filter:Gezo => Boolean = _ => true):RDD[Gezo] = {

    val rawGezo = sc.textFile(fileName)

    rawGezo
      .zipWithIndex
      .filter(_._2 > 0)
      .map(_._1)
      .map(_.split("\t"))
      .map(r =>
        new Gezo(
          r(0),
          r(1),
          r(2),
          parseOptionString(Try(r(3)).toOption),
          parseOptionString(Try(r(4)).toOption),
          parseOptionString(Try(r(5)).toOption),
          parseOptionString(Try(r(6)).toOption),
          parseOptionString(Try(r(7)).toOption),
          parseOptionString(Try(r(8)).toOption),
          parseOptionString(Try(r(9)).toOption),
          parseOptionString(Try(r(10)).toOption),
          parseOptionString(Try(r(11)).toOption),
          parseOptionString(Try(r(12)).toOption),
          parseOptionString(Try(r(13)).toOption),
          parseOptionString(Try(r(14)).toOption)
        )
      )
      .filter(filter)
      .cache
  }

  def lidanoFilterGezo(selection:Set[String])(record: Gezo):Boolean = {
    selection contains record.lidano
  }

  def lidanoFilterFarma(selection:Set[String])(record: Farma):Boolean = {
    selection contains record.lidano
  }

  def loadPrestDictionary(sc:SparkContext, prestDictString:String):scala.collection.Map[String,String] = {

    sc.textFile(prestDictString)
      .zipWithIndex
//      .filter(_._2 > 0)
      .keys
      .map(x => x.split("\\|").map(_.replace("\"", "")))
      .map(x => (x(0), x(3)))
      .collectAsMap

  }

    def loadDictionary(sc:SparkContext, atcDictString:String, atcDict7String:String):scala.collection.Map[String,String] = {

    val atcDict = sc.textFile(atcDictString)
      .zipWithIndex
      .filter(_._2 > 0)
      .keys
      .map(_.split(";").map(_.replace("\"", "")))
      .map(x => (Try(x(0).toInt).toOption, x(4)))
      .flatMap(x => x._1 match {
        case Some(i) => Some(i.toString, x._2)
        case None => None
      })
      .collectAsMap

    val atcDict7 = sc.textFile(atcDict7String)
      .zipWithIndex
      .filter(_._2 > 0)
      .keys
      .map(_.split("\t").map(_.replace("\"", "")))
      .flatMap(x => Seq((Try(x(4)).toOption, Try(x(7).toInt).toOption),
        (Try(x(4)).toOption, Try(x(8).toInt).toOption),
        (Try(x(4)).toOption, Try(x(9).toInt).toOption),
        (Try(x(4)).toOption, Try(x(10).toInt).toOption)))
      .filter(_._1.isDefined)
      .map { case (atc, ocnk) => ocnk match {
        case Some(cnk) => Some(cnk.toString, atc.get)
        case None => None
      }
      }
      .flatMap(x => x)
      .collectAsMap

    val diffAtcNotInNew = atcDict.filter{case (key, value) => value != "-" && !atcDict7.contains(key)}.toMap

    atcDict7 ++ diffAtcNotInNew

  }

  def loadDictionaryNames(sc:SparkContext, atcDictString:String, atcDict7String:String):scala.collection.Map[String,String] = {

    val atcDict = sc.textFile(atcDictString)
      .zipWithIndex
      .filter(_._2 > 0)
      .keys
      .map(_.split(";").map(_.replace("\"", "")))
      .map(x => (Try(x(0).toInt).toOption, x(1)))
      .flatMap(x => x._1 match {
        case Some(i) => Some(i.toString, x._2)
        case None => None
      })
      .collectAsMap

    val atcDict7 = sc.textFile(atcDict7String)
      .zipWithIndex
      .filter(_._2 > 0)
      .keys
      .map(_.split("\t").map(_.replace("\"", "")))
      .flatMap(x => Seq((Try(x(1)).toOption, Try(x(7).toInt).toOption),
        (Try(x(1)).toOption, Try(x(8).toInt).toOption),
        (Try(x(1)).toOption, Try(x(9).toInt).toOption),
        (Try(x(1)).toOption, Try(x(10).toInt).toOption)))
      .filter(_._1.isDefined)
      .map { case (name, ocnk) => ocnk match {
        case Some(cnk) => Some(cnk.toString, name.get)
        case None => None
      }
      }
      .flatMap(x => x)
      .collectAsMap

    val diffAtcNotInNew = atcDict.filter{case (key, value) => value != "-" && !atcDict7.contains(key)}.toMap

    atcDict7 ++ diffAtcNotInNew

  }

  def createHistoriesFarma(farma:RDD[Farma]):RDD[TimelineFarma] = {
    // val paddedGezo = padGezo(gezo)

    // Efficient gathering of the troups, better than groupBy
    val intermediate:RDD[((String,String), List[Farma])] = farma
      .keyBy(x => (x.lidano, x.baDat))
      .combineByKey(
        (value) => List(value),
        (aggr:List[Farma], value) => aggr ::: value :: Nil,
        (aggr1:List[Farma], aggr2:List[Farma]) => aggr1 ::: aggr2
      )
      .sortBy(x => x._1)

    // Convert to object model
    intermediate
      .map{case ((lidano, baDat), lGezo) =>
        TimelineFarma(TimelineFarmaKey(lidano, baDat), lGezo, Map())}
      .cache

  }

  def getTimeLineFarma(histories:RDD[TimelineFarma], user:String, dateRange:Option[(String, String)] = None) = {
    histories
      .filter(_.key.lidano == user)
      .filter{case TimelineFarma(TimelineFarmaKey(lidano, date), _, _) => dateRange match {
        case Some((begin, end)) => date >= begin && date <= end
        case None => true
      }
      }
      .map{case TimelineFarma(TimelineFarmaKey(lidano, date), events, meta) =>
        List(lidano, date, events.length, meta.toString)
      }
      .collect
  }

  /**
    * Add a begin record and end record to every persons' records.
    */
  def padGezo(sc:SparkContext, gezo: RDD[Gezo], beginDate:String = "20120101", endDate:String = "2161231"):RDD[Gezo] = {
    val lidanos = gezo.map(_.lidano).distinct.collect

    val addRddBegin = sc.parallelize(lidanos.map(l => new Gezo(l, beginDate, "0", None, None, None, None, None, None, None, None, None, None, None, None)))
    val addRddEnd = sc.parallelize(lidanos.map(l => new Gezo(l, endDate, "0", None, None, None, None, None, None, None, None, None, None, None, None)))

    gezo union addRddBegin union addRddEnd
  }

  def createHistoriesGezo(sc:SparkContext)(gezo:RDD[Gezo]):RDD[TimelineGezo] = {
    val paddedGezo = padGezo(sc, gezo)

    // Efficient gathering of the troups, better than groupBy
    val intermediate:RDD[((String,String), List[Gezo])] = gezo
      .keyBy(x => (x.lidano, x.baDat))
      .combineByKey(
        (value) => List(value),
        (aggr:List[Gezo], value) => aggr ::: value :: Nil,
        (aggr1:List[Gezo], aggr2:List[Gezo]) => aggr1 ::: aggr2
      )
      .sortBy(x => x._1)

    // Convert to object model
    intermediate
      .map{case ((lidano, baDat), lGezo) =>
        TimelineGezo(TimelineGezoKey(lidano, baDat), lGezo, Map())}
      .cache

  }

  // Annotate the history with information about entering, being and leaving the hospital
  // Most of the entries can be covered using a sliding window, keeping in mind that the first and last element need to be padded.
  // This has been done earlier in the pipeline.
  // Be careful, requires historyRdd to be sorted!

  def annotateIsHospital(hist:RDD[TimelineGezo]):RDD[TimelineGezo] = {
    hist
      .map(x => x.copy(meta = Map("hospital" -> x.events.map(_.hnummer.isDefined).reduce(_||_))))
  }

  def annotateIsHospitalWindow(hist:RDD[TimelineGezo]):RDD[TimelineGezo] = {
    hist
      .sliding(3)
      .map(_.toList)
      .map{case List(a, b, c) => List(a,b,c).map(_.meta.getOrElse("hospital", false)) match {
        case false :: true :: true  :: Nil  => b.copy(meta = b.meta ++ Map("firstDay" -> true))
        case true  :: true :: false :: Nil  => b.copy(meta = b.meta ++ Map("lastDay" -> true))
        case true  :: true :: true :: Nil   => b.copy(meta = b.meta ++ Map("midDay" -> true))
        case false :: true :: false :: Nil  => b.copy(meta = b.meta ++ Map("1day" -> true))
        case false :: false :: false :: Nil => b
        case _                              => b
      }
      }
  }

  def parseAmounts(bed:Option[String], perstus: Option[String], gev:Option[String]) = {
    val bedrag = parseOptionString(bed).map(_.toDouble).getOrElse(0.0)
    val persoonlijk = parseOptionString(perstus).map(_.toDouble).getOrElse(0.0)
    val aantal = parseOptionString(gev).map(_.toInt).getOrElse(0)
    // (bedrag, persoonlijk, aantal)
    val totaal = Math.max(bedrag, persoonlijk)

    Map(
      "totaal" -> totaal,
      "ziv"    -> (totaal - persoonlijk),
      "gev"    -> aantal
    )
  }

  def parseAmountsToTuple(bed:Option[String], perstus: Option[String], gev:Option[String]) = {
    val bedrag = bed.map(_.toDouble).getOrElse(0.0)
    val persoonlijk = perstus.map(_.toDouble).getOrElse(0.0)
    val aantal = gev.map(_.toInt).getOrElse(0)
    // (bedrag, persoonlijk, aantal)
    val totaal = Math.max(bedrag, persoonlijk)

    (totaal, (totaal - persoonlijk), aantal)
  }

  def sumAmountsFarma(a: Array[Farma]) = {
    val (totaal, perstus, gev) =
      a
        .map(x => parseAmountsToTuple(x.bed, x.perstus, x.gev))
        .map{case (totaal, perstus, gev) => (totaal * gev, perstus * gev, gev)}
        .reduce((x,y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))

    Map("totaal" -> totaal, "perstus" -> perstus, "ziv" -> (totaal - perstus))
  }

  def sumAmountsGezo(a: Array[Gezo]) = {
    val (totaal, perstus, gev) =
      a
        .map(x => parseAmountsToTuple(x.bed, x.perstus, x.gev))
        .map{case (totaal, perstus, gev) => (totaal * gev, perstus * gev, gev)}
        .reduce((x,y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))

    Map("totaal" -> totaal, "perstus" -> perstus, "ziv" -> (totaal - perstus))
  }

}
