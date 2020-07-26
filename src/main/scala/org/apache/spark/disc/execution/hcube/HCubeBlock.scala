package org.apache.spark.disc.execution.hcube

import org.apache.spark.disc.catlog.Catalog.DataType
import org.apache.spark.disc.catlog.Schema
import org.apache.spark.disc.execution.subtask.utils.Trie

abstract class HCubeBlock(val schema: Schema, val shareVector: Array[Int])
    extends Serializable {}

case class TupleHCubeBlock(override val schema: Schema,
                           override val shareVector: Array[Int],
                           content: Array[Array[DataType]])
    extends HCubeBlock(schema, shareVector) {}

case class TrieHCubeBlock(override val schema: Schema,
                          override val shareVector: Array[Int],
                          content: Trie)
    extends HCubeBlock(schema, shareVector) {}

class TableContent

class RowTableContent

class ColumnTableContent
