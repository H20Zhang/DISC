package org.apache.spark.Logo.Plan

case class HNumberDecider(val patternMap:Map[String,Int], val lazyMap:Map[String,Boolean], val tree:QTree) {

  lazy val costString = {
    tree.costString(patternMap,lazyMap)
  }

  lazy val constraint = {
    val varibles = AttributesMap.translate(tree.root.attr)
    val stringBuilder = new StringBuilder

    varibles.foldLeft(stringBuilder)((builder,v) => builder.append(s"*x($v)"))
    stringBuilder.deleteCharAt(0)
    stringBuilder.append(s"-${patternMap("Machine")*10}")
    stringBuilder.append(";")

    varibles.foldLeft(stringBuilder)((builder,v) => builder.append(s"x($v)-1;"))
  }

  lazy val initInput = {
    val varibles = AttributesMap.translate(tree.root.attr)
    val stringBuilder = new StringBuilder

    varibles.foldLeft(stringBuilder)((builder,varible) => builder.append(s"1;"))
  }

  def costProgramming = {
    s"""
       |#!octave -qf
       |1;
       |
       |function r = g (x)
       | r = [
       |  $constraint
       | ];
       |endfunction
       |
       |function obj = phi(x)
       |  obj = $costString;
       |endfunction
       |
       |x0 = [$initInput];
       |[x, obj, info, iter, nf, lambda] = sqp (x0, @phi, [], @g,[],[],100)
       |
     """.stripMargin
  }


  def invokeOctave(): Unit ={
    val outputFile = ""
  }


}
