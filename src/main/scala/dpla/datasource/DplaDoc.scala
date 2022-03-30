package dpla.datasource

case class DplaDoc(
  uri: String, //@id
  id: String,
  dataProvider: Seq[NamedEntity],
  hasView: Seq[WebResource],
  iiifManifest: Option[String],
  intermediateProvider: Option[String],
  isPartOf: Seq[WebResource], // necessary?
  isShownAt: Option[String],
  mediaMaster: Seq[String],
  `object`: Seq[String],
  preview: Seq[WebResource], // option?
  provider: Option[NamedEntity],
  rights: Option[String],
  sourceResource: SourceResource,
  typeError: Seq[TypeError]
)

case class WebResource(
  uri: Option[String], //@id
  format: Seq[String],
  rights: Seq[String],
  edmRights: Option[String],
  isReferencedBy: Option[String]
)

case class SourceResource(
  collection: Seq[Collection],
  contributor: Seq[String],
  creator: Seq[String],
  date: Seq[Date],
  description: Seq[String],
  extent: Seq[String],
  format: Seq[String],
  genre: Seq[String],
  identifier: Seq[String],
  isPartOf: Seq[WebResource],
  language: Seq[Language],
  publisher: Seq[String],
  relation: Seq[String],
  rights: Seq[String],
  specType: Seq[String],
  spatial: Seq[Spatial],
  stateLocatedIn: Seq[NamedEntity],
  subject: Seq[NamedEntity],
  temporal: Seq[Date],
  title: Seq[String],
  `type`: Seq[String]
)

case class Collection(
  uri: Option[String], //@id
  id: Option[String],
  title: Seq[String],
  description: Option[String]
)

case class Date(
  begin: Option[String],
  end: Option[String],
  displayDate: Option[String]
)

case class NamedEntity(
  uri: Option[String],
  name: Option[String],
  exactMatch: Seq[String]
)

case class Language(
  `iso639_3`: Option[String],
  name: Option[String]
)

case class Spatial(
  name: Option[String],
  city: Option[String],
  state: Option[String],
  county: Option[String],
  region: Option[String],
  country: Option[String],
  coordinates: Seq[String], //confirmed
  `iso3166_2`: Option[String]
)

case class TypeError(
  expected: String,
  actual: String,
  field: String = ""
)
