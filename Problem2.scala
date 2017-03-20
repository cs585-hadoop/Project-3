import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer

object Problem2 {
  
  
  def calcCell(input:String):Array[Int]={
    val s:Array[String]=input.split(",");
    val x=((s(0).toDouble)/20).toInt+1
    val y=499-((s(1).toDouble)/20).toInt
    val index=x+500*y
    val list = ListBuffer[Int](index,index+1,index-1,index+500,index-500,index+501,index-501,index+499,index-499)
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
      var num=9;
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
    val spark = SparkSession.builder()
      .appName("Problem2")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext    

    val textFileF = sc.textFile("/home/mqp/p/p.txt")

    val cells = textFileF.map(line=>calcCell(line)).flatMap(line=>line).map(word=>(word,1)).reduceByKey((x,y) => x + y)
    val rdi=cells.map(line=>avg(line)).sortBy(_._2,false).take(50).foreach(println)
  }
}
