package org.apache.spark.adj.execution.subtask

import org.apache.spark.adj.database.Catalog.DataType
import org.apache.spark.adj.execution.hcube.TrieHCubeBlock

class TrieConstructedLeapFrogJoin(trieTask: TrieConstructedLeapFrogJoinSubTask)
    extends LeapFrogJoin(trieTask) {

//  override protected val initV: Unit = {
//    init()
//  }

  override def init(): Unit = {

    tries = trieTask.tries.map(_.asInstanceOf[TrieHCubeBlock].content).toArray
    //as trie has already been constructed, only iterators needed to be init.
    initIterators()
  }

}
