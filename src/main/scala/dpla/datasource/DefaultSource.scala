package dpla.datasource

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._

class DefaultSource extends RelationProvider {

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]) : DplaDataRelation = {

    val query: String = parameters.getOrElse("query", "")

    new DplaDataRelation(query)(sqlContext)
  }
}
