package org.apache.spark.adj.database.util

import org.apache.spark.adj.database.Relation

abstract class StatisticComputer {

}

class CardinalityComputer extends StatisticComputer {

  def cardinality(relation:Relation) = {
    relation.content.count()
  }

}

class DegreeComputer extends StatisticComputer {

}
