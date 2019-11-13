package dpla.datasource

import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.elasticsearch.spark._
import scala.collection.mutable.LinkedHashMap
import scala.collection.Map

class DplaDataRelation (query: String)
                  (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Serializable {

  override def schema: StructType = {
    val docType = ScalaReflection.schemaFor[DplaDoc].dataType.asInstanceOf[StructType]
    val docField = StructField("doc", docType, true)
    StructType(Seq(docField))
  }

  /**
    * Build the rows for the DataFrame.
    */
  override def buildScan(): RDD[Row] = {

    // TODO: user-defined query params
    val configs = Map(
      "spark.es.nodes" -> "search-prod1-es7.internal.dp.la",
      "spark.es.mapping.date.rich" -> "false",
      "spark.es.resource" -> "dpla_alias/item",
      "spark.es.query" -> query
    )

    val rdd: RDD[(String, Map[String, AnyRef])] =
      sqlContext.sparkContext.esRDD(configs)

    val docs: RDD[Map[String, AnyRef]] = rdd.map{ case (k, v) => v }

    val dplaDocs: RDD[DplaDoc] = docs.map { doc =>

      val sourceResource: (SourceResource, Seq[TypeError]) = getSourceResource(getMapOpt(doc.get("sourceResource")))

      val provider = getNamedEntityOpt(getMapOpt(doc.get("provider")))
      val intermediateProvider = getStringOpt(doc.get("intermediateProvider"))
      val isShownAt = getStringOpt(doc.get("isShownAt"))
      val rights = getStringOpt(doc.get("rights"))

      val dataProvider = getStringSeq(doc.get("dataProvider"))
      val `object` = getStringSeq(doc.get("object"))
      val hasView = getWebResourceSeq(getMapSeq(doc.get("hasView")))
      val isPartOf = getWebResourceSeq(getMapSeq(doc.get("isPartOf")))
      val preview = getWebResourceSeq(getMapSeq(doc.get("preview")))

      val typeErrors: Seq[TypeError] = Seq(
        hasView.map(z => handleTypeErrorSeq(z._2, "hasView")),
        isPartOf.map(z => handleTypeErrorSeq(z._2, "isPartOf")),
        preview.map(z => handleTypeErrorSeq(z._2, "preview"))
      ).flatten.flatten.union(
        Seq(
          handleTypeErrorSeq(sourceResource._2, "sourceResource"),
          handleTypeErrorSeq(provider.map(_._2).getOrElse(Seq()), "provider")
        ).flatten.union(
          Seq(
            handleTypeErrorOpt(dataProvider.left.toOption, "dataProvider"),
            handleTypeErrorOpt(intermediateProvider.left.toOption, "intermediateProvider"),
            handleTypeErrorOpt(isShownAt.left.toOption, "isShownAt"),
            handleTypeErrorOpt(`object`.left.toOption, "object"),
            handleTypeErrorOpt(rights.left.toOption, "rights")
          ).flatten
        )
      )

      DplaDoc(
        uri = doc("@id").asInstanceOf[String],
        id = doc("id").asInstanceOf[String],
        dataProvider = dataProvider.right.getOrElse(Seq()),
        hasView = hasView.map(_._1),
        intermediateProvider = intermediateProvider.right.getOrElse(None),
        isPartOf = isPartOf.map(_._1),
        isShownAt = isShownAt.right.getOrElse(None),
        `object` = `object`.right.getOrElse(Seq()),
        preview = preview.map(_._1),
        provider = provider.map(_._1),
        rights = rights.right.getOrElse(None),
        sourceResource = sourceResource._1,
        typeError = typeErrors
      )
    }

    dplaDocs.map{ r => Row(r) }
  }

  def getStringOpt(x: Option[Any]): Either[TypeError, Option[String]] = {
    x match {
      case Some(a) => a match {
        case None => Left(TypeError(expected = "String", actual = "None"))
        case null => Left(TypeError(expected = "String", actual = "null"))
        case b: String => Right(Some(b))
        case _: Seq[_] => Left(TypeError(expected = "String", actual = "Seq[_]"))
        case _ => Left(TypeError(expected = "String", actual = a.getClass.toString))
      }
      case None => Right(None)
    }
  }

  def getStringSeq(x: Option[Any]): Either[TypeError, Seq[String]] = {
    x match {
      case Some(a) => a match {
        case None => Left(TypeError(expected = "Seq[String]", actual = "None"))
        case null => Left(TypeError(expected = "Seq[String]", actual = "null"))
        case b: String => Right(Seq(b))
        case c: Seq[_] => {
          val types = innerSeqTypes(c)

          if (types.size == 1 && types(0) == "String")
            // This should not throw exception b/c we just checked that all
            // members of c are Strings.
            Right(c.asInstanceOf[Seq[String]])
          else
            Left(TypeError(
              expected = "Seq[String]",
              actual = "Seq[" + types.mkString(", ") + "]"
            ))
        }
        case _ =>
          Left(TypeError(expected = "Seq[String]", actual = a.getClass.toString))
      }
      case None => Right(Seq())
    }
  }

  def innerSeqTypes(x: Seq[Any]): Seq[String] = {
    x.map(a =>
      a match {
        case None => "None"
        case null => "null"
        case _: String => "String"
        case _: Seq[_] => "Seq[_]"
        case _ => "_"
      }
    ).distinct.sorted
  }

  def getMapOpt(x: Option[Any]): Option[LinkedHashMap[String, Any]] = {
    try x.asInstanceOf[Option[LinkedHashMap[String, Any]]]
    catch { case _: Exception => None }
  }

  def getMapSeq(x: Option[Any]): Seq[LinkedHashMap[String, Any]] = {
    val a: Seq[_] = x match {
      case Some(b) => b match {
        case c: LinkedHashMap[_,_] => Seq(c)
        case d: Seq[_] => d
        case _ => Seq[LinkedHashMap[String, Any]]()
      }
      case None => Seq[LinkedHashMap[String, Any]]()
    }

    // Cast to correct type and filter out null values.
    try a.asInstanceOf[Seq[LinkedHashMap[String, Any]]].filter(e => e != null)
    catch { case _: Exception => Seq[LinkedHashMap[String, Any]]() }
  }

  def getNamedEntityOpt(x: Option[LinkedHashMap[String, Any]]): Option[(NamedEntity, Seq[TypeError])] =
    x.map(y => getNamedEntity(y))

  def getNamedEntitySeq(x: Seq[LinkedHashMap[String, Any]]): Seq[(NamedEntity, Seq[TypeError])] =
    x.map(y => getNamedEntity(y))

  def getNamedEntity(y: LinkedHashMap[String, Any]): (NamedEntity, Seq[TypeError]) = {
    val uri = getStringOpt(y.get("@id"))
    val name= getStringOpt(y.get("name"))

    val namedEntity = NamedEntity(
      uri = uri.right.toOption.flatten,
      name = name.right.toOption.flatten
    )

    val typeErrors: Seq[TypeError] = Seq(
      handleTypeErrorOpt(name.left.toOption, "uri"),
      handleTypeErrorOpt(name.left.toOption, "name")
    ).flatten

    (namedEntity, typeErrors)
  }

  def getWebResourceSeq(x: Seq[LinkedHashMap[String, Any]]): Seq[(WebResource, Seq[TypeError])] = {
    x.map(y => {
      val uri = getStringOpt(y.get("@id"))
      val format = getStringSeq(y.get("format"))
      val rights = getStringSeq(y.get("rights"))
      val edmRights = getStringOpt(y.get("edmRights"))
      val isReferencedBy = getStringOpt(y.get("isReferencedBy"))

      val typeErrors: Seq[TypeError] = Seq(
        handleTypeErrorOpt(uri.left.toOption, "uri"),
        handleTypeErrorOpt(format.left.toOption, "format"),
        handleTypeErrorOpt(rights.left.toOption, "rights"),
        handleTypeErrorOpt(edmRights.left.toOption, "edmRights"),
        handleTypeErrorOpt(isReferencedBy.left.toOption, "isReferencedBy")
      ).flatten

      val webResource = WebResource(
        uri = uri.right.getOrElse(None),
        format = format.right.getOrElse(Seq()),
        rights = rights.right.getOrElse(Seq()),
        edmRights = edmRights.right.getOrElse(None),
        isReferencedBy = isReferencedBy.right.getOrElse(None)
      )

      (webResource, typeErrors)
    })
  }

  def getSourceResource(x: Option[LinkedHashMap[String, Any]]): (SourceResource, Seq[TypeError]) = {

    val sr: LinkedHashMap[String, Any] =
      x.getOrElse(LinkedHashMap[String, Any]())

    val contributor = getStringSeq(sr.get("contributor"))
    val creator = getStringSeq(sr.get("creator"))
    val description = getStringSeq(sr.get("description"))
    val extent = getStringSeq(sr.get("extent"))
    val format = getStringSeq(sr.get("format"))
    val genre = getStringSeq(sr.get("genre"))
    val identifier = getStringSeq(sr.get("identifier"))
    val publisher = getStringSeq(sr.get("publisher"))
    val relation = getStringSeq(sr.get("relation"))
    val rights = getStringSeq(sr.get("rights"))
    val specType = getStringSeq(sr.get("specType"))
    val title = getStringSeq(sr.get("title"))
    val `type` = getStringSeq(sr.get("type"))

    val collection = getCollectionSeq(getMapSeq(sr.get("collection")))
    val date = getDateSeq(getMapSeq(sr.get("date")))
    val isPartOf = getWebResourceSeq(getMapSeq(sr.get("isPartOf")))
    val language = getLanguageSeq(getMapSeq(sr.get("language")))
    val spatial = getSpatialSeq(getMapSeq(sr.get("spatial")))
    val stateLocatedIn = getNamedEntitySeq(getMapSeq(sr.get("stateLocatedIn")))
    val subject = getNamedEntitySeq(getMapSeq(sr.get("subject")))
    val temporal = getDateSeq(getMapSeq(sr.get("temporal")))

    val typeErrors: Seq[TypeError] = Seq(
      collection.map(z => handleTypeErrorSeq(z._2, "collection")),
      date.map(z => handleTypeErrorSeq(z._2, "date")),
      isPartOf.map(z => handleTypeErrorSeq(z._2, "isPartOf")),
      language.map(z => handleTypeErrorSeq(z._2, "language")),
      spatial.map(z => handleTypeErrorSeq(z._2, "spatial")),
      stateLocatedIn.map(z => handleTypeErrorSeq(z._2, "stateLocatedIn")),
      subject.map(z => handleTypeErrorSeq(z._2, "subject")),
      temporal.map(z => handleTypeErrorSeq(z._2, "temporal"))
    ).flatten.flatten.union(
      Seq(
        handleTypeErrorOpt(contributor.left.toOption, "contributor"),
        handleTypeErrorOpt(creator.left.toOption, "creator"),
        handleTypeErrorOpt(description.left.toOption, "description"),
        handleTypeErrorOpt(extent.left.toOption, "extent"),
        handleTypeErrorOpt(format.left.toOption, "format"),
        handleTypeErrorOpt(genre.left.toOption, "genre"),
        handleTypeErrorOpt(identifier.left.toOption, "identifier"),
        handleTypeErrorOpt(publisher.left.toOption, "publisher"),
        handleTypeErrorOpt(relation.left.toOption, "relation"),
        handleTypeErrorOpt(rights.left.toOption, "rights"),
        handleTypeErrorOpt(specType.left.toOption, "specType"),
        handleTypeErrorOpt(title.left.toOption, "title"),
        handleTypeErrorOpt(`type`.left.toOption, "type")
      ).flatten
    )

    val sourceResource = SourceResource(
      contributor = contributor.right.getOrElse(Seq()),
      creator = creator.right.getOrElse(Seq()),
      description = description.right.getOrElse(Seq()),
      extent = extent.right.getOrElse(Seq()),
      format = format.right.getOrElse(Seq()),
      genre = genre.right.getOrElse(Seq()),
      identifier = identifier.right.getOrElse(Seq()),
      publisher = publisher.right.getOrElse(Seq()),
      relation = relation.right.getOrElse(Seq()),
      rights = rights.right.getOrElse(Seq()),
      specType = specType.right.getOrElse(Seq()),
      title = title.right.getOrElse(Seq()),
      `type` = `type`.right.getOrElse(Seq()),
      collection = collection.map(_._1),
      date = date.map(_._1),
      isPartOf = isPartOf.map(_._1),
      language = language.map(_._1),
      spatial = spatial.map(_._1),
      stateLocatedIn = stateLocatedIn.map(_._1),
      subject = subject.map(_._1),
      temporal = temporal.map(_._1)
    )

    (sourceResource, typeErrors)
  }

  def getCollectionSeq(x: Seq[LinkedHashMap[String, Any]]): Seq[(Collection, Seq[TypeError])] = {
    x.map(y => {
      val uri = getStringOpt(y.get("@id"))
      val id = getStringOpt(y.get("id"))
      val title = getStringSeq(y.get("title"))
      val description = getStringOpt(y.get("description"))

      val typeErrors: Seq[TypeError] = Seq(
        handleTypeErrorOpt(uri.left.toOption, "uri"),
        handleTypeErrorOpt(id.left.toOption, "id"),
        handleTypeErrorOpt(title.left.toOption, "title"),
        handleTypeErrorOpt(description.left.toOption, "description")
      ).flatten

      val collection = Collection(
        uri = uri.right.getOrElse(None),
        id = id.right.getOrElse(None),
        title = title.right.getOrElse(Seq()),
        description = description.right.getOrElse(None)
      )

      (collection, typeErrors)
    })
  }

  def getDateSeq(x: Seq[LinkedHashMap[String, Any]]): Seq[(Date, Seq[TypeError])] = {
    x.map(y => {
      val begin = getStringOpt(y.get("begin"))
      val end = getStringOpt(y.get("end"))
      val displayDate = getStringOpt(y.get("displayDate"))

      val typeErrors: Seq[TypeError] = Seq(
        handleTypeErrorOpt(begin.left.toOption, "begin"),
        handleTypeErrorOpt(end.left.toOption, "end"),
        handleTypeErrorOpt(displayDate.left.toOption, "displayDate")
      ).flatten

      val date = Date(
        begin = begin.right.getOrElse(None),
        end = end.right.getOrElse(None),
        displayDate = displayDate.right.getOrElse(None)
      )

      (date, typeErrors)
    })
  }

  def getLanguageSeq(x: Seq[LinkedHashMap[String, Any]]): Seq[(Language, Seq[TypeError])] = {
    x.map(y => {

      val name = getStringOpt(y.get("name"))
      val iso639_3 = getStringOpt(y.get("iso639_3"))

      val typeErrors: Seq[TypeError] = Seq(
        handleTypeErrorOpt(name.left.toOption, "name"),
        handleTypeErrorOpt(iso639_3.left.toOption, "iso639_3")
      ).flatten

      val language = Language(
        name = name.right.getOrElse(None),
        iso639_3 = `iso639_3`.right.getOrElse(None)
      )

      (language, typeErrors)
    })
  }

  def getSpatialSeq(x: Seq[LinkedHashMap[String, Any]]): Seq[(Spatial, Seq[TypeError])] = {
    x.map(y => {

      val name = getStringOpt(y.get("name"))
      val city = getStringOpt(y.get("city"))
      val county = getStringOpt(y.get("county"))
      val state = getStringOpt(y.get("state"))
      val country = getStringOpt(y.get("country"))
      val region = getStringOpt(y.get("region"))
      val coordinates = getStringSeq(y.get("coordinates"))
      val iso3166_2 = getStringOpt(y.get("iso3166-2"))

      val typeErrors: Seq[TypeError] = Seq(
        handleTypeErrorOpt(name.left.toOption, "name"),
        handleTypeErrorOpt(city.left.toOption, "city"),
        handleTypeErrorOpt(county.left.toOption, "county"),
        handleTypeErrorOpt(state.left.toOption, "state"),
        handleTypeErrorOpt(country.left.toOption, "country"),
        handleTypeErrorOpt(region.left.toOption, "region"),
        handleTypeErrorOpt(coordinates.left.toOption, "coordinates"),
        handleTypeErrorOpt(iso3166_2.left.toOption, "iso3166_2")
      ).flatten

      val spatial = Spatial(
        name = name.right.getOrElse(None),
        city = city.right.getOrElse(None),
        county = county.right.getOrElse(None),
        state = state.right.getOrElse(None),
        country = country.right.getOrElse(None),
        region = region.right.getOrElse(None),
        coordinates = coordinates.right.getOrElse(Seq()),
        `iso3166_2` = iso3166_2.right.getOrElse(None)
      )

      (spatial, typeErrors)
    })
  }

  def handleTypeErrorOpt(typeError: Option[TypeError], field: String): Option[TypeError] = {
    typeError match {
      case Some(e) => Some(reLabelError(e, field))
      case None => None
    }
  }

  def handleTypeErrorSeq(typeErrors: Seq[TypeError], field: String): Seq[TypeError] =
    typeErrors.map(x => reLabelError(x, field))

  def reLabelError(x: TypeError, field: String): TypeError =
    TypeError(
      actual = x.actual,
      expected = x.expected,
      field = Seq(field, x.field).filter(_ != "").mkString(".")
    )
}
