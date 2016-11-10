package tetraitesapi

import scala.util.Try
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import Model._

object Common extends Serializable {

  def parseOptionString(os: Option[String]):Option[String] = os match {
    case None => None
    case Some("") => None
    case _ => os
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

  def loadDictionary(sc:SparkContext):scala.collection.Map[String,String] = {

    val atcDict = sc.textFile("/Users/toni/Dropbox/_KUL/LCM/tetraites/tetraitesAPI/src/resources/ATCDPP.CSV")
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

    val atcDict7 = sc.textFile("/Users/toni/Dropbox/_KUL/LCM/tetraites/tetraitesAPI/src/resources/atcCodes.txt")
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

}
