package tetraitesapi

/**
  * The necessary class and object info for tetraitesAPI
  */
object Model extends Serializable {

  case class Farma(val lidano: String,
                   val prestatie: String,
                   val farmprod: Option[String],
                   val modn: Option[String],
                   val baDat: String,
                   val dateInd: Option[String],
                   val bed: Option[String],
                   val gev: Option[String],
                   val perstus: Option[String],
                   val lnum1: Option[String],
                   val lnum2: Option[String],
                   val verstrNr: Option[String],
                   val verstrSpec: Option[String],
                   val voorsNr: Option[String],
                   val voorsSpec: Option[String],
                   val apothekerTia: Option[String],
                   val apothekerSpec: Option[String] ) extends Serializable {

    def asMap:Map[String,String] = {
        Map(
            "lidano" -> lidano,
            "prestatie" -> prestatie,
            "farmprod" -> farmprod.getOrElse(""),
            "modn" -> modn.getOrElse(""),
            "baDat" -> baDat,
            "dateInd" -> dateInd.getOrElse(""),
            "bed" -> bed.getOrElse(""),
            "gev" -> gev.getOrElse(""),
            "perstus" -> perstus.getOrElse(""),
            "lnum1" -> lnum1.getOrElse(""),
            "lnum2" -> lnum2.getOrElse(""),
            "verstrNr" -> verstrNr.getOrElse(""),
            "verstrSpec" -> verstrSpec.getOrElse(""),
            "voorsNr" -> voorsNr.getOrElse(""),
            "voorsSpec" -> voorsSpec.getOrElse(""),
            "apothekerTia" -> apothekerTia.getOrElse(""),
            "apothekerSpec" -> apothekerSpec.getOrElse("")
        )
    }

  }

  case class TimelineFarmaKey(val lidano:String, val baDat: String) extends Serializable

  case class TimelineFarma(
                            val key: TimelineFarmaKey,
                            val events: List[Farma],
                            val meta: Map[String,Boolean] = Map()
                          ) extends Serializable

  case class Gezo(val lidano: String,
                  val baDat: String,
                  val prestatie: String,
                  val farmprod: Option[String],
                  val bed: Option[String],
                  val gev: Option[String],
                  val hnummer: Option[String],
                  val lnum2: Option[String],
                  val modn: Option[String],
                  val norm2: Option[String],
                  val perstus: Option[String],
                  val verstrNr: Option[String],
                  val verstrSpec: Option[String],
                  val voorsNr: Option[String],
                  val voorsSpec: Option[String]) extends Serializable {
    
    def asMap:Map[String,String] = {
      Map(
        "lidano" ->lidano,
        "baDat" -> baDat,
        "prestatie" -> prestatie,
        "farmprod" -> farmprod.getOrElse(""),
        "bed" -> bed.getOrElse(""),
        "gev" -> gev.getOrElse(""),
        "hnummer" -> hnummer.getOrElse(""),
        "lnum2" -> lnum2.getOrElse(""),
        "modn" -> modn.getOrElse(""),
        "nom2" -> norm2.getOrElse(""),
        "perstus" -> perstus.getOrElse(""),
        "verstrNr" -> verstrNr.getOrElse(""),
        "verstrSpec" -> verstrSpec.getOrElse(""),
        "voorsNr" -> voorsNr.getOrElse(""),
        "voorsSpec" -> voorsSpec.getOrElse("")
      )
    }
  }

  case class TimelineGezoKey(val lidano:String, val baDat: String) extends Serializable

  case class TimelineGezo(
                           val key: TimelineGezoKey,
                           val events: List[Gezo],
                           val meta: Map[String,Boolean] = Map()
                         ) extends Serializable

}
