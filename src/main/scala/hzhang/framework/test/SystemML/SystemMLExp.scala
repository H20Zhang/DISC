package hzhang.framework.test.SystemML

import org.apache
import org.apache.spark
import org.apache.spark.Logo.UnderLying.utlis.SparkSingle
import org.apache.sysml.api.mlcontext._
import org.apache.sysml.api.mlcontext.ScriptFactory._
import org.apache.sysml.api.mlcontext.{MLContext, MatrixMetadata}
import org.apache.sysml.api.mlcontext.ScriptFactory.dml
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.sysml.api.ml.LogisticRegression

import scala.util.Random

class SystemMLExp {
  val spark = SparkSingle.getSparkSession()
  val sc = SparkSingle.getSparkContext()
  val ml = new MLContext(spark)
  def testBasic(){

    val helloScript = dml("print('hello world')")
    ml.execute(helloScript)

//    val clf = ml.nn.examples.Mnist_lenet
//    val dummy = clf.generate_dummy_data
//    val dummyVal = clf.generate_dummy_data
//    val params = clf.train(dummy.X, dummy.Y, dummyVal.X, dummyVal.Y, dummy.C, dummy.Hin, dummy.Win, 1)
//    val probs = clf.predict(dummy.X, dummy.C, dummy.Hin, dummy.Win, params.W1, params.b1, params.W2, params.b2, params.W3, params.b3, params.W4, params.b4)
//    val perf = clf.eval(probs, dummy.Y)
  }

  def testMatrix(): Unit ={

    //explain the execution
    //ml.setExplain(true)

    //create a matrix
    val numRows = 10000
    val numCols = 100
    val data = sc.parallelize(0 to numRows-1).map { _ => Row.fromSeq(Seq.fill(numCols)(Random.nextDouble)) }
    val schema = StructType((0 to numCols-1).map { i => StructField("C" + i, DoubleType, true) } )
    val df = spark.createDataFrame(data, schema)

    //There are four types in script "double, integer, string, and boolean"
    val showTypes =
      """
        |aDouble = 3.0
        |bInteger = 2
        |print('aDouble = ' + aDouble)
        |print('bInteger = ' + bInteger)
        |print('aDouble + bInteger = ' + (aDouble + bInteger))
        |print('bInteger ^ 3 = ' + (bInteger ^ 3))
        |print('aDouble ^ 2 = ' + (aDouble ^ 2))
        |
        |cBoolean = TRUE
        |print('cBoolean = ' + cBoolean)
        |print('(2 < 1) = ' + (2 < 1))
        |
        |dString = 'Hello'
        |eString = dString + ' World'
        |print('dString = ' + dString)
        |print('eString = ' + eString)
      """.stripMargin
    val showTypeScript = dml(showTypes)
    ml.execute(showTypeScript)

    //find min max mean of matrix
    val minMaxMean =
      """
        minOut = min(Xin)
        maxOut = max(Xin)
        meanOut = mean(Xin)
      """.stripMargin

    val mm = new MatrixMetadata(numRows, numCols)
    val minMaxMeanScript = dml(minMaxMean).in("Xin", df, mm).out("minOut", "maxOut", "meanOut")
    val (min, max, mean) = ml.execute(minMaxMeanScript).getTuple[Double, Double, Double]("minOut", "maxOut", "meanOut")
    println(min, max, mean)
    minMaxMeanScript.clearAll()

  //create matrix inside dml and print
    val s =
      """
      m = matrix("11 22 33 44", rows=2, cols=2)
      n = sum(m)

      for (i in 1:nrow(m)) {
      for (j in 1:ncol(m)) {
              n1 = m[i,j]
              print('[' + i + ',' + j + ']:' + as.scalar(n1))
          }
      }
      """.stripMargin

    val scr = dml(s).out("m", "n");
    val res = ml.execute(scr)
    val (x, y) = res.getTuple[Matrix, Double]("m", "n")


    val readWriteMatrix =
      """
        |m = matrix("1 2 3 0 0 0 7 8 9 0 0 0", rows=4, cols=3)
        |      write(m, "m.csv", format="csv")
        |
        |      m = read("m.csv")
        |      print("min:" + min(m))
        |      print("max:" + max(m))
        |      print("sum:" + sum(m))
        |      mRowSums = rowSums(m)
        |      for (i in 1:nrow(mRowSums)) {
        |     print("row " + i + " sum:" + as.scalar(mRowSums[i,1]))
        |      }
        |      mColSums = colSums(m)
        |      for (i in 1:ncol(mColSums)) {
        |     print("col " + i + " sum:" + as.scalar(mColSums[1,i]))
        |      }
      """.stripMargin

    val readWriteMatrixScript = dml(readWriteMatrix)
    ml.execute(readWriteMatrixScript)

    //"t":Transpose, "%*%":matrix multiply, $+$:matrix add,
    val matrixOperation =
      """
        |A = matrix("1 2 3 4 5 6", rows=3, cols=2)
        |print(toString(A))
        |B = A + 4
        |B = t(B)
        |print(toString(B))
        |C = A %*% B
        |print(toString(C))
        |D = matrix(5, rows=nrow(C), cols=ncol(C))
        |D = (C - D) / 2
        |print(toString(D))
        |
        |A = matrix("1 2 3 4 5 6 7 8 9", rows=3, cols=3)
        |print(toString(A))
        |B = A[3,3]
        |print(toString(B))
        |C = A[2,]
        |print(toString(C))
        |D = A[,3]
        |print(toString(D))
        |E = A[2:3,1:2]
        |print(toString(E))
      """.stripMargin

    val matrixOperationScript = dml(matrixOperation)
    ml.execute(matrixOperationScript)


    val controlFlowAndFunction =
      """
        |i = 1
        |while (i <= 3) {
        |    if (i == 1) {
        |        print('hello')
        |    } else if (i == 2) {
        |        print('world')
        |    } else {
        |        print('!!!')
        |    }
        |    i = i + 1
        |}
        |
        |A = matrix("1 2 3 4 5 6", rows=3, cols=2)
        |
        |for (i in 1:nrow(A)) {
        |    print("for A[" + i + ",1]:" + as.scalar(A[i,1]))
        |}
        |
        |parfor(i in 1:nrow(A)) {
        |    print("parfor A[" + i + ",1]:" + as.scalar(A[i,1]))
        |}
        |
        |doSomething = function(matrix[double] mat) return (matrix[double] ret) {
        |    additionalCol = matrix(1, rows=nrow(mat), cols=1) # 1x3 matrix with 1 values
        |    ret = cbind(mat, additionalCol) # concatenate column to matrix
        |    ret = cbind(ret, seq(0, 2, 1))  # concatenate column (0,1,2) to matrix
        |    ret = cbind(ret, rowMaxs(ret))  # concatenate column of max row values to matrix
        |    ret = cbind(ret, rowSums(ret))  # concatenate column of row sums to matrix
        |}
        |
        |A = rand(rows=3, cols=2, min=0, max=2) # random 3x2 matrix with values 0 to 2
        |B = doSomething(A)
        |write(A, "A.csv", format="csv")
        |write(B, "B.csv", format="csv")
      """.stripMargin
    val controlFlowAndFunctionScript = dml(controlFlowAndFunction)
    ml.execute(controlFlowAndFunctionScript)
    //print the information of script src
//    print(scr.info())

    //clear the name reference hold inside script src
    scr.clearAll()



  }

  def testRDDIntegration(): Unit ={
    val rdd1 = sc.parallelize(Array("1.0,2.0", "3.0,4.0"))
    val rdd2 = sc.parallelize(Array("5.0,6.0", "7.0,8.0"))
    val sums = """
      s1 = sum(m1);
      s2 = sum(m2);
if (s1 > s2) {
  message = "s1 is greater"
} else if (s2 > s1) {
  message = "s2 is greater"
} else {
  message = "s1 and s2 are equal"
}
"""
    scala.tools.nsc.io.File("sums.dml").writeAll(sums)
    val sumScript = dmlFromFile("sums.dml").in(Map("m1"-> rdd1, "m2"-> rdd2)).out("s1", "s2", "message")
    val sumResults = ml.execute(sumScript)
    val s1 = sumResults.getDouble("s1")
    val s2 = sumResults.getDouble("s2")
    val message = sumResults.getString("message")
    println(message)
  }

  def testHaberman(): Unit ={
    val habermanUrl = "http://archive.ics.uci.edu/ml/machine-learning-databases/haberman/haberman.data"
    val habermanList = scala.io.Source.fromURL(habermanUrl).mkString.split("\n")
    val habermanRDD = sc.parallelize(habermanList)
    val habermanMetadata = new MatrixMetadata(306, 4)
    val typesRDD = sc.parallelize(Array("1.0,1.0,1.0,2.0"))
    val typesMetadata = new MatrixMetadata(1, 4)
    val scriptUrl = "https://raw.githubusercontent.com/apache/systemml/master/scripts/algorithms/Univar-Stats.dml"
    val uni = dmlFromUrl(scriptUrl).in("A", habermanRDD, habermanMetadata).in("K", typesRDD, typesMetadata).in("$CONSOLE_OUTPUT", true)
    ml.execute(uni)
  }

//  def testMLAlgorithm(): Unit ={
//    val lr = new LogisticRegression("logReg", sc).setIcpt(0).setMaxOuterIter(100).setMaxInnerIter(0).setRegParam(0.000001).setTol(0.000001)
//    val model = lr.fit(X_train_df)
//    val prediction = model.transform(X_test_df)
//  }
}


