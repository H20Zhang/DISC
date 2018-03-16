package org.apache.spark.Logo.Plan.LogicalPlan

import java.io.PrintWriter

import scala.sys.process._

case class HNumberDecider(val patternMap:Map[String,Int], val lazyMap:Map[String,Boolean], val tree:QTree, val index:Int) {

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

    varibles.foldLeft(stringBuilder)((builder,varible) => builder.append(s"2;"))
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
       |[x, obj, info, iter, nf, lambda] = sqp (x0, @phi, [], @g,[],[],100);
       |disp(x')
       |disp(obj)
     """.stripMargin
  }


  def invokeOctave() ={
    val outputFile = s"./temp${index}.m"
    val filePrinter = new PrintWriter(outputFile)
    filePrinter.println(costProgramming)
    filePrinter.close()

    val cmd = s"chmod +x ./temp${index}.m".!
    val cmd2 = s"octave -qf ./temp${index}.m".!!

    val res = cmd2.split("\n")
    val xValues = res(0).split("\\s").filter(p => !p.isEmpty).map(f => f.toDouble)
    val objValues = res(1).split("\\s").filter(p => !p.isEmpty).map(f => f.toDouble).apply(0)

    val cmd3 = s"rm ./temp${index}.m".!

    (xValues,objValues)

  }


}
