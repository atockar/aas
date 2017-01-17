import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._

// Load in data and map to LabeledPoint
val rawData = sc.textFile("hdfs:///user/ds/covtype.data")
val data = rawData.map { line =>
 val values = line.split(',').map(_.toDouble)
 val featureVector = Vectors.dense(values.init)
 val label = values.last - 1
 LabeledPoint(label, featureVector)
}

// Train-cv-test split
val Array(trainData, cvData, testData) = data.randomSplit(Array(0.8, 0.1, 0.1))
trainData.cache()
cvData.cache()
testData.cache()

// Train decision tree classfier and check metrics
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd._
def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]): MulticlassMetrics = {
 val predictionsAndLabels = data.map(x => (model.predict(x.features), x.label))
 new MulticlassMetrics(predictionsAndLabels)
}
val model = DecisionTree.trainClassifier( trainData, 7, Map[Int,Int](), "gini", 4, 100)
val metrics = getMetrics(model, cvData)
metrics.confusionMatrix
metrics.precision
(0 until 7).map(cat => (metrics.precision(cat), metrics.recall(cat))).foreach(println)

// Random model
import org.apache.spark.rdd._
def classProbabilities(data: RDD[LabeledPoint]): Array[Double] = {
 val countsByCategory = data.map(_.label).countByValue()
 val counts = countsByCategory.toArray.sortBy(_._1).map(_._2)
 counts.map(_.toDouble / counts.sum)
}
val trainPriorProbabilities = classProbabilities(trainData)
val cvPriorProbabilities = classProbabilities(cvData)
trainPriorProbabilities.zip(cvPriorProbabilities).map {
 case (trainProb, cvProb) => trainProb * cvProb
}.sum

// Tune hyperparameters - grid search
val evaluations =
 for (impurity <- Array("gini", "entropy");
      depth <- Array(1, 20);
      bins <- Array(10, 300))
  yield {
   val model = DecisionTree.trainClassifier(trainData, 7, Map[Int,Int](), impurity, depth, bins)
   val predictionsAndLabels = cvData.map(x => (model.predict(x.features), x.label))
   val accuracy = new MulticlassMetrics(predictionsAndLabels).precision
   ((impurity, depth, bins), accuracy)
  }
evaluations.sortBy(_._2).reverse.foreach(println)

// Best model, including cvData
val model = DecisionTree.trainClassifier(trainData.union(cvData), 7, Map[Int,Int](), "entropy", 20, 300)

// Reload data with alternate encoding of categorical features (number rather than one-hot [for speed])
val data = rawData.map { line =>
 val values = line.split(',').map(_.toDouble)
 val wilderness = values.slice(10, 14).indexOf(1.0).toDouble
 val soil = values.slice(14, 54).indexOf(1.0).toDouble
 val featureVector = Vectors.dense(values.slice(0, 10) :+ wilderness :+ soil)
 val label = values.last - 1
 LabeledPoint(label, featureVector)
}

// Redo grid search, noting the Map criterion in the model that specifies categorical variables
val evaluations =
 for (impurity <- Array("gini", "entropy");
      depth <- Array(10, 20, 30);
      bins <- Array(40, 300))
  yield {
   val model = DecisionTree.trainClassifier(trainData, 7, Map(10 -> 4, 11 -> 40), impurity, depth, bins)
   val trainAccuracy = getMetrics(model, trainData).precision
   val cvAccuracy = getMetrics(model, cvData).precision
   ((impurity, depth, bins), (trainAccuracy, cvAccuracy))
  }

// Random forests
val forest = RandomForest.trainClassifier(trainData, 7, Map(10 -> 4, 11 -> 40), 20, "auto", "entropy", 30, 300)

// Make predictions
val input = "2709,125,28,67,23,3224,253,207,61,6094,0,29"
val vector = Vectors.dense(input.split(',').map(_.toDouble))
forest.predict(vector)