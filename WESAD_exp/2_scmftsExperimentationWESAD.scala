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


//SCMFTS applitcation on WESAD dataset.
val vars=Array("w_BVP","c_ACCx","c_ACCy","c_ACCz" ,"c_ECG","c_EMG","c_EDA","c_TEMP","c_RESP","w_ACCx","w_ACCy","w_ACCz","w_EDA","w_TEMP")

for( varSel <- vars ){
	println(varSel)
	
  //Step 1-2:
	val varSelBroadcast = sc.broadcast(varSel)
	val rawDataTrain=sc.textFile("hdfs://****/WESAD_Var_"+varSel+".csv")
	val train = rawDataTrain.filter(line => !line.contains("subject")).map{line =>
	    val array = line.split(",")
	    array.map(f => f.toDouble) 
	}.repartition(17*19*3)

	train.cache()
	train.count()

  //Step 3:
	timer.start(varSel)
	val out=train.mapPartitions { partition =>
		val R = org.ddahl.rscala.RClient()
		val localTrain = partition.toArray

		R.eval("""
			myPaths <- .libPaths()
			myPaths <- c('/home/root/R/x86_64-pc-linux-gnu-library/3.6/',myPaths)
			freqs=c(rep(700,8),rep(32,3),64,4,4)
			names(freqs)=c('c_ACCx','c_ACCy','c_ACCz' ,'c_ECG','c_EMG','c_EDA','c_TEMP','c_RESP','w_ACCx','w_ACCy','w_ACCz','w_BVP','w_EDA','w_TEMP')
		""")
		R.eval(".libPaths(myPaths)")
		R.eval("data=%-",localTrain)		
		R.eval("var=%-",varSelBroadcast.value)

		val out=R.evalD2("t(apply(data,1,function(row){c(row[c(1,2,3)],unlist(scmfts::scmfts(list(ts(unlist(row[-c(1,2,3)]),frequency = freqs[var])),n_cores=1,scale=T,fast=F)))}))").toIterator
		R.quit()
		out
	}
	out.cache()
	out.count()
	timer.stop(varSel)

	//Output SCMFTS files
	out.map(x=>x.mkString(",")).repartition(1).saveAsTextFile("hdfs://****/WESAD_cmfts_Var_"+varSel+".csv")
	out.count

	train.unpersist()
	out.unpersist()
	print(timer.toString)
	import java.io.PrintWriter
	new PrintWriter("****/timeCMFTSScaled.txt") { write(timer.toString); close }
}


//Data preparation for step 4:
val vars=Array("w_BVP","c_ACCx","c_ACCy","c_ACCz" ,"c_ECG","c_EMG","c_EDA","c_TEMP","c_RESP","w_ACCx","w_ACCy","w_ACCz","w_EDA","w_TEMP")
var rdds:Array[org.apache.spark.rdd.RDD[(String, Array[String])]]=Array.fill[org.apache.spark.rdd.RDD[(String, Array[String])]](vars.length)(sc.emptyRDD)
for(varSel <- 0 to (vars.length-1)){
	println(varSel)
	var rdd1=sc.textFile("hdfs://****/WESAD_cmfts_Var_"+vars(varSel)+".csv")
	var	rdd2=rdd1.map{line =>
	    	val array = line.split(",")
	    	array
			}.map(line => (line(1),line)).repartition(17*19*3)

	if(varSel==0){

	}else{
		rdd2=rdd2.map(x=> (x._1,x._2.drop(3)))
	}
	rdds(varSel)=rdd2
}

//Step 4:
var out=rdds(0)
timer.start("Union")
for(varSel <- 1 to (vars.length-1)){
	println(varSel)
	out=out.join(rdds(varSel)).map(a=>(a._1,a._2._1++a._2._2))
	//out.cache
	out.count
}
out.cache
out.count
timer.stop("Union")
print(timer.toString)
//out contains the data of step 5.
import spark.implicits._
out.toDF.write.csv("/****/WESAD_CMFTS_normal_RAW_numeric.csv")
