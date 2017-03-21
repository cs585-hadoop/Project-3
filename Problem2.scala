import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer

object Problem2 {
  
  def getLocation(input:String):Int={
    val s:Array[String]=input.split(",");
    val x=((s(0).toDouble)/20).toInt+1
    val y=499-((s(1).toDouble)/20).toInt
    val index=x+500*y
    return index   
  }
  
  
  def getNeighbors(input:String):Array[Int]={
    val s:Array[String]=input.split(",");
    val x=((s(0).toDouble)/20).toInt+1
    val y=499-((s(1).toDouble)/20).toInt
    val index=x+500*y
    val list = ListBuffer[Int](index+1,index-1,index+500,index-500,index+501,index-501,index+499,index-499)
    if(x==1){
      list-=(index-1,index-501,index+499)        
    }
    if(x==500){
      list-=(index+1,index+501,index-499)        
    }
    if(y==0){
      list-=(index-500,index-501,index-499)        
    }
    if(y==499){
      list-=(index+500,index+501,index+499)        
    }
    return list.toList.toArray
  }
  
   def avg( a:(Int, Int)) : (Int, Float)= {
      var num=8;
      if(a._1%500==1)
        num-=3
      if(a._1%500==0)
        num-=3
      if(a._1>=1 && a._1<=500)
        num-=3
      if(a._1>=249500 && a._1<=250000)
        num-=3
      if(a._1==1||a._1==500||a._1==249500||a._1==250000)
        num+=1
      return (a._1,a._2.toFloat/num)
   }  
  

  def main(args: Array[String]) {
    if(args.length<1){
      println("Please Input Path!")
      return
    }
    val spark = SparkSession.builder()
      .appName("Problem2")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext    

    val textFileF = sc.textFile(args(0),4).cache
    val cell = textFileF.map(line=>(getLocation(line),1)).reduceByKey((x,y) => x + y)
    val avgNeighbor = textFileF.map(line=>getNeighbors(line)).flatMap(line=>line).map(word=>(word,1)).reduceByKey((x,y) => x + y).map(line=>avg(line))
    val rdi=cell.join(avgNeighbor,4).map(line=>(line._1,line._2._1/line._2._2)).sortBy(_._2,false).take(50).foreach(println)
  }
}
