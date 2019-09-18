package org.apache.spark.adj.execution.hcube

import org.apache.spark.adj.database.Catalog.DataType
import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.execution.subtask.utils.Trie

abstract class HCubeBlock(val schema: RelationSchema,
                          val shareVector: Array[Int])
    extends Serializable {}

case class TupleHCubeBlock(override val schema: RelationSchema,
                           override val shareVector: Array[Int],
                           content: Array[Array[DataType]])
    extends HCubeBlock(schema, shareVector) {}

case class TrieHCubeBlock(override val schema: RelationSchema,
                          override val shareVector: Array[Int],
                          content: Trie)
    extends HCubeBlock(schema, shareVector) {}

class TableContent

class RowTableContent

class ColumnTableContent
