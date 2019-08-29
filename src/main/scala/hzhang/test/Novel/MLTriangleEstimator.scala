package hzhang.test.Novel

import org.apache.spark.adj.utils.SparkSingle
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector, Vectors}




class MLTriangleEstimator {

}

class SparkMLUsage {

  lazy val spark = SparkSingle.getSparkSession()

  // Load the data stored in LIBSVM format as a DataFrame.
  val data = spark.read.format("libsvm").load("sample_libsvm_data.txt")

  // Split the data into training and test sets (30% held out for testing)
  val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 1234L)


  def testDataStructure(): Unit ={
    // Create a dense vector (1.0, 0.0, 3.0).
    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
    val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
    val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))

    // Create a labeled point with a positive label and a dense feature vector.
    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))

    // Create a labeled point with a negative label and a sparse feature vector.
    val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))

    // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))

    // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))

//    println(dv)
//    println(sv1)
//    println(pos)
//    println(neg)
    println(dm)
    println(sm)
  }

  def testDistributedDataStructure: Unit ={

  }


  def testModel(model:ProbabilisticClassificationModel[_,_]): Unit ={

    // Select example rows to display.
    val predictions = model.transform(testData)
    predictions.show()

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test set accuracy = " + accuracy)
  }

  def testLogisticalRegression(): Unit ={

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val model = lr.fit(data)

    testModel(model)

  }

  def testBayes(): Unit ={


    // Train a NaiveBayes model.
    val model = new NaiveBayes()
      .fit(trainingData)

    testModel(model)
  }


}


