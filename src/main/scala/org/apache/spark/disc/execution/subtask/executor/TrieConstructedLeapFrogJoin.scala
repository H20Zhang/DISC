package org.apache.spark.disc.execution.subtask.executor

import org.apache.spark.disc.execution.hcube.TrieHCubeBlock
import org.apache.spark.disc.execution.subtask.TrieConstructedLeapFrogJoinSubTask

class TrieConstructedLeapFrogJoin(trieTask: TrieConstructedLeapFrogJoinSubTask)
    extends LeapFrogJoin(trieTask) {

  override def init(): Unit = {

    tries = trieTask.tries.map(_.asInstanceOf[TrieHCubeBlock].content).toArray
    //as trie has already been constructed, only iterators needed to be init.
    initIterators()
  }

}
