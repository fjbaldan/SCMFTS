//Time Tracker used:

import scala.collection.mutable.{ HashMap => MutableHashMap }
import org.apache.spark.annotation.Experimental

/**
 * Time tracker implementation which holds labeled timers.
 */
@Experimental
class TimeTracker extends Serializable {

  private val starts: MutableHashMap[String, Long] = new MutableHashMap[String, Long]()

  private val totals: MutableHashMap[String, Long] = new MutableHashMap[String, Long]()

  /**
   * Starts a new timer, or re-starts a stopped timer.
   */
  def start(timerLabel: String): Unit = {
    val currentTime = System.nanoTime()
    if (starts.contains(timerLabel)) {
      throw new RuntimeException(s"TimeTracker.start(timerLabel) called again on" +
        s" timerLabel = $timerLabel before that timer was stopped.")
    }
    starts(timerLabel) = currentTime
  }

  /**
   * Stops a timer and returns the elapsed time in seconds.
   */
  def stop(timerLabel: String): Double = {
    val currentTime = System.nanoTime()
    if (!starts.contains(timerLabel)) {
      throw new RuntimeException(s"TimeTracker.stop(timerLabel) called on" +
        s" timerLabel = $timerLabel, but that timer was not started.")
    }
    val elapsed = currentTime - starts(timerLabel)
    starts.remove(timerLabel)
    if (totals.contains(timerLabel)) {
      totals(timerLabel) += elapsed
    } else {
      totals(timerLabel) = elapsed
    }
    elapsed / 1e9
  }

  /**
   * Print all timing results in seconds.
   */
  override def toString: String = {
    totals.map {
      case (label, elapsed) =>
        s"  $label: ${elapsed / 1e9}"
    }.mkString("\n")
  }
}

var timer=new TimeTracker()
//Accruacy evaluation
sc.setLogLevel("ERROR")

//RDD to Dataframes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.{LabeledPoint => NewLabeledPoint}

//RF
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}


var evaluatorACC = new MulticlassClassificationEvaluator()
.setLabelCol("label")
.setPredictionCol("prediction")
.setMetricName("accuracy")

var evaluatorF1 = new MulticlassClassificationEvaluator()
.setLabelCol("label")
.setPredictionCol("prediction")
.setMetricName("f1")

def trainRF(dfTrain: org.apache.spark.sql.Dataset[NewLabeledPoint],
    dfTest: org.apache.spark.sql.Dataset[NewLabeledPoint], numTrees:Int, maxDepth:Int, seed:Int,
    minInstancesPerNode:Int, maxBins:Int): (Double,Double) = {
    val rf = new RandomForestClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setNumTrees(numTrees)
    .setMaxDepth(maxDepth)
    .setSeed(seed)
    .setMinInstancesPerNode(minInstancesPerNode)
    .setMaxBins(maxBins)

    val model = rf.fit(dfTrain)
    val predictions = model.transform(dfTest)
    val labelAndPreds=predictions.select("label","prediction")
    (evaluatorACC.evaluate(predictions),evaluatorF1.evaluate(predictions))
}

//DT
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}

def trainDT(dfTrain: org.apache.spark.sql.Dataset[NewLabeledPoint],
 dfTest: org.apache.spark.sql.Dataset[NewLabeledPoint]): (Double,Double) = {
    val dt = new DecisionTreeClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setMaxDepth(10)
        .setSeed(1)
        .setMinInstancesPerNode(20)

    val dtModel = dt.fit(dfTrain)
    val predictions = dtModel.transform(dfTest)
    val labelAndPreds=predictions.select("label","prediction")
    
    (evaluatorACC.evaluate(predictions),evaluatorF1.evaluate(predictions))
}


val indices= 0 to 14
val subjects=List("2","3","4","5","6","7","8","9","10","11","13","14","15","16","17").map(x=>x.toDouble)

val resultsRF:Array[(Double,Double)]=Array.fill[(Double,Double)](subjects.length)((0.0,0.0))
val resultsDT:Array[(Double,Double)]=Array.fill[(Double,Double)](subjects.length)((0.0,0.0))

val data=sc.textFile("hdfs://****/WESAD_CMFTS_normal_RAW_numeric_inputedNA.csv")

// Train & Test RDDs
val df = data.filter(line => !line.contains("V")).map{line =>
    val array = line.split(",")
    var arrayDouble = array.map(f => f.toDouble) 
    val label = arrayDouble(2) 
    //val featureVector = Vectors.dense(arrayDouble) 
    val featureVector = Vectors.dense(arrayDouble.drop(3)) 
    (arrayDouble(0),LabeledPoint(label-1, featureVector))
}.repartition(19)

df.cache
df.count	

for( indice <- indices ){
  val subject =subjects(indice)
  println(subject);
    
	var test =df.filter(x=>x._1==subject).map(x=>x._2)
	var train =df.filter(x=>x._1!=subject).map(x=>x._2)

	//Binary case
  val binario=0
  if(binario==1){
      println("Binario")
      train=train.map{ x =>
          if(x.label==2.0){
              LabeledPoint(0.0, x.features)
          }else{
              LabeledPoint(x.label,x.features)
          }
      }

      test=test.map{ x =>
          if(x.label==2.0){
              LabeledPoint(0.0, x.features)
          }else{
              LabeledPoint(x.label,x.features)
          }
      }
  }
   
  train.persist()
	test.persist()
	test.count
	train.count

	val dfTrain = train.map(l => NewLabeledPoint(l.label, l.features.asML)).toDS()
  val dfTest = test.map(l => NewLabeledPoint(l.label, l.features.asML)).toDS()

	dfTrain.persist()
	dfTest.persist()
	dfTest.count
	dfTrain.count

  println("RF")
  timer.start("RF")
  resultsRF(indice)=trainRF(dfTrain,dfTest,100,10,1,20,32)
  timer.stop("RF")
  
  println("DT")
  timer.start("DT")
  resultsDT(indice)=trainDT(dfTrain,dfTest)
  timer.stop("DT")

  println(timer.toString)

  train.unpersist()
  test.unpersist()

  dfTrain.unpersist()
  dfTest.unpersist()
}

println("Acc F1")
println(timer)
timer.map(x=>x._1).sum / timer.length
timer.map(x=>x._2).sum / timer.length

println("Acc F1")
println(resultsDT)
resultsDT.map(x=>x._1).sum / resultsRF.length
resultsDT.map(x=>x._2).sum / resultsRF.length

println(timer.toString)

import java.io._
val write = new PrintWriter(new FileOutputStream(new File("/****/TimeAccRFDT.txt"),true))
write.write("resultsRF\n")
write.write("ACC "+(resultsRF.map(x=>x._1).sum / resultsRF.length).toString+"\n")
write.write("F1 "+(resultsRF.map(x=>x._2).sum / resultsRF.length).toString+"\n")
write.write("resultsDT\n")
write.write("ACC "+(resultsDT.map(x=>x._1).sum / resultsDT.length).toString+"\n")
write.write("F1 "+(resultsDT.map(x=>x._2).sum / resultsDT.length).toString+"\n")
write.close()


//KNN
import org.apache.spark.ml.classification._
import org.apache.spark.mllib.util._

val data=sc.textFile("hdfs://****/WESAD_CMFTS_normal_RAW_numeric_inputedNA.csv")

// Train & Test RDDs
val df = data.filter(line => !line.contains("V")).map{line =>
    val array = line.split(",")
    var arrayDouble = array.map(f => f.toDouble) 
    val label = arrayDouble(2) 
    //val featureVector = Vectors.dense(arrayDouble) 
    val featureVector = Vectors.dense(arrayDouble.drop(3)) 
    (arrayDouble(0),LabeledPoint(label-1, featureVector))
}.repartition(19)

val TotalKnn=df.map(x=>x._2)

df.cache
df.count  

TotalKnn.cache
TotalKnn.count

val dfTotalKnn = TotalKnn.map(l => NewLabeledPoint(l.label, l.features.asML)).toDS()

TotalKnn.cache
TotalKnn.count
dfTotalKnn.cache
dfTotalKnn.count

import org.apache.spark.ml.feature.StandardScaler

val scaler = new StandardScaler()
  .setInputCol("features")
  .setOutputCol("scaledFeatures")
  .setWithStd(true)
  .setWithMean(false)

// Compute summary statistics by fitting the StandardScaler.
val scalerModel = scaler.fit(dfTotalKnn)

val indices= 0 to 14
val subjects=List("2","3","4","5","6","7","8","9","10","11","13","14","15","16","17").map(x=>x.toDouble)

val resultsknn:Array[(Double,Double)]=Array.fill[(Double,Double)](subjects.length)((0.0,0.0))

for( indice <- indices ){
  val subject =subjects(indice)
  println(subject);
    
  var test =df.filter(x=>x._1==subject).map(x=>x._2)
  var train =df.filter(x=>x._1!=subject).map(x=>x._2)

  //Binary Case
  val binario=1
  if(binario==1){
      println("Binario")
      train=train.map{ x =>
          if(x.label==2.0){
              LabeledPoint(0.0, x.features)
          }else{
              LabeledPoint(x.label,x.features)
          }
      }

      test=test.map{ x =>
          if(x.label==2.0){
              LabeledPoint(0.0, x.features)
          }else{
              LabeledPoint(x.label,x.features)
          }
      }
  }

  train.persist()
  test.persist()
  test.count
  train.count

  val dfTrain = train.map(l => NewLabeledPoint(l.label, l.features.asML)).toDS()
  val dfTest = test.map(l => NewLabeledPoint(l.label, l.features.asML)).toDS()

  dfTrain.persist()
  dfTest.persist()
  dfTest.count
  dfTrain.count

  // Normalize Train & Test
  // Normalize each feature to have unit standard deviation.
  var trainScaled = scalerModel.transform(dfTrain).drop("features").withColumnRenamed("scaledFeatures","features")
  var testScaled = scalerModel.transform(dfTest).drop("features").withColumnRenamed("scaledFeatures","features")

  trainScaled.persist()
  testScaled.persist()
  testScaled.count
  trainScaled.count

  val knn = new KNNClassifier()
    .setTopTreeSize(trainScaled.count().toInt / 500)
    .setK(9)

  timer.start("knn9")
  val knnModel = knn.fit(trainScaled)
  val predictions = knnModel.transform(testScaled)
  predictions.count
  timer.stop("knn9")

  val labelAndPreds=predictions.select("label","prediction")
  
  import org.apache.spark.mllib.evaluation._

  resultsknn(indice)= (evaluatorACC.evaluate(predictions),evaluatorF1.evaluate(predictions))

  println(timer.toString)
  println(resultsknn(indice))

  train.unpersist()
  test.unpersist()
  dfTrain.unpersist()
  dfTest.unpersist()
  trainScaled.unpersist()
  testScaled.unpersist()
}

import java.io._
val write = new PrintWriter(new FileOutputStream(new File("/****/TimeAcc_knn.txt"),true))
write.write(resultsknn.mkString(","))
write.write("\n")
write.write("Tiempos"+"\n"+timer.toString)
write.write("\n")
write.write("resultsKNN\n")
write.write("ACC "+(resultsknn.map(x=>x._1).sum / resultsknn.length).toString+"\n")
write.write("F1 "+(resultsknn.map(x=>x._2).sum / resultsknn.length).toString+"\n")
write.close()

