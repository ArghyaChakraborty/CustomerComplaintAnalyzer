import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
 * This Class tries to accomplish the following -
 * 1) get total# of complaints received for each category in each month of year
 * 2) get total# of complaints received for each category for each company in each month of year
 * 3) get total# of complaints received for each category for each company in each month of year & how many of them are closed
 * 4) get total# of complaints received for each category for each company in each month of year & for how many of them timely response is provided
 */


object CustomerComplaintAnalyzer {
  
  def main(args: Array[String]) {
    if(args.length < 2) {
      println("Usage : spark-submit --class CustomerComplaintAnalyzer customer_complaint_analyzer_<version>.jar <input file path> <input file name> [<output file path>]")
    } else {
      println("Processing Started")
      
      val ipPath = args(0)
      val ipFile = args(1)
      val opPath = if(args.length >= 3) args(2) else ipPath
      
      var time = System.currentTimeMillis()
      
      val sConf = new SparkConf().setAppName("Customer Complaint Analyzer").setMaster("local[*]")
      val sc = new SparkContext(sConf)
      
      // Get the input file RDD
      val fileRdd = sc.textFile(ipPath+"/"+ipFile)
      
      // Get rid of bad lines / records by checking -
      // whether the line is a header 
      // whether the line is a blank line
      // whether the line has reqd. number of values
      // whether the line is a complete one or broken into multiple lines (in that case , reject it)
      val goodRdd = fileRdd.filter(line => !isHeader(line)).filter(line => line.trim().length() != 0).filter(line => line.split(",").size > 15).filter(line => hasEvenNumQuotes(line))
      
      println("Pre-processing completed in : "+((System.currentTimeMillis()-time)/1000)+"seconds")
      time = System.currentTimeMillis()
      
      // 1)
      goodRdd.map(line => {val lineArr = line.split(","); (getMonthNYearTuple(lineArr(0)),lineArr(1))}).map(tup => (new String(tup._1._1+","+tup._1._2+","+tup._2),1)).reduceByKey(_+_).map(tup => new String(tup._1+","+tup._2)).saveAsTextFile(opPath+"/Customer_Complaint_Count_In_Year_Month")
      println("Step# 1 completed in : "+((System.currentTimeMillis()-time)/1000)+"seconds")
      time = System.currentTimeMillis()
      
      // 2) 
      val totCompComlRdd = goodRdd.
                            map(line => {
                              var lineArr = Array("") 
                              if(line.contains("\"")) {
                                lineArr = formatSpecialLines(line).split(",")
                               } else {
                                 lineArr = line.split(",")
                               }
                              (getMonthNYearTuple(lineArr(0)),lineArr(1), lineArr(7))
                             }).
                            map(tup => (new String(tup._1._1+","+tup._1._2+","+tup._2+","+tup._3),1)).
                            reduceByKey(_+_)
      totCompComlRdd.map(tup => new String(tup._1+","+tup._2)).saveAsTextFile(opPath+"/Customer_Complaint_Count_In_Category_In_Year_Month")
      println("Step# 2 completed in : "+((System.currentTimeMillis()-time)/1000)+"seconds")
      time = System.currentTimeMillis()
      
      // 3)
      val totCompComlClsRdd = goodRdd.
                                map(line => {
                                  var lineArr = Array("")
                                  if(line.contains("\"")) {
                                    lineArr = formatSpecialLines(line).split(",")
                                  } else {
                                    lineArr = line.split(",")
                                  }
                                  (getMonthNYearTuple(lineArr(0)),lineArr(1), lineArr(7), lineArr(14))
                                 }).
                                map(tup => 
                                  if(tup._4.toLowerCase().contains("close")){
                                    (new String(tup._1._1+","+tup._1._2+","+tup._2+","+tup._3),1)
                                  } else {
                                    (new String(tup._1._1+","+tup._1._2+","+tup._2+","+tup._3),0)
                                  }).
                                reduceByKey(_+_)

      // totCompComlClsRdd.foreach(println)

      val totCompCompVsClsRdd = totCompComlRdd.join(totCompComlClsRdd).map(tup => new String(tup._1+","+tup._2._1+","+tup._2._2))

      // totCompCompVsClsRdd.foreach(println)
      totCompCompVsClsRdd.saveAsTextFile(opPath+"/Customer_Complaint_Count_In_Category_In_Year_Month_Vs_Close_Count")
      println("Step# 3 completed in : "+((System.currentTimeMillis()-time)/1000)+"seconds")
      time = System.currentTimeMillis()
      
      // 4) 
      val totCompComlTimrsRdd = goodRdd.
                                  map(line => {
                                    var lineArr = Array("")
                                    if(line.contains("\"")) {
                                      lineArr = formatSpecialLines(line).split(",")
                                    } else {
                                      lineArr = line.split(",")
                                    }
                                    (getMonthNYearTuple(lineArr(0)),lineArr(1), lineArr(7), lineArr(15))
                                   }).
                                   map(tup => 
                                     if(tup._4.toLowerCase().contains("yes")){
                                       (new String(tup._1._1+","+tup._1._2+","+tup._2+","+tup._3),1)
                                     } else {
                                       (new String(tup._1._1+","+tup._1._2+","+tup._2+","+tup._3),0)
                                     }).
                                   reduceByKey(_+_)

      // totCompComlTimrsRdd.foreach(println)

      val totCompCompVsTimrsRdd = totCompComlRdd.join(totCompComlTimrsRdd).map(tup => new String(tup._1+","+tup._2._1+","+tup._2._2))

      // totCompCompVsTimrsRdd.foreach(println)
      totCompCompVsTimrsRdd.saveAsTextFile(opPath+"/Customer_Complaint_Count_In_Category_In_Year_Month_Vs_Concented_Count")
      println("Step# 4 completed in : "+((System.currentTimeMillis()-time)/1000)+"seconds")
      time = System.currentTimeMillis()
      
      println("Processing Completed")
    }
  }
  
  
  // This method returns (mm,yyyy) tuple from a string of mm/dd/yyyy
  def getMonthNYearTuple(str : String) : (String,String) = {
    val strArr = str.split("/")
    (strArr(0),strArr(2))
  }
  
  // This method determines whether a line is a header or a data line by checkin whether the first field is a date field
  def isHeader(line : String) : Boolean = {
    val splittedLine = line.split(",")
    val pattern = "[0-9]+/[0-9]+/[0-9]+".r
    splittedLine(0) match {
      case pattern() => false
      case _ => true
    }
  }
  
  // This method deals with lines having the separator within the values. Ex. if the seperator is comma (,) & one of the value is "ABC Corp, LLC" 
  // then this method - i) deletes the double quotes surrounding the value ii) replaces the comma by blank value
  // Therefore : "ABC Corp, LLC" => ABC Corp LLC
  def formatSpecialLines(str : String) : String = {
    val charArr = str.toCharArray()
    val sbf = new StringBuffer()
    var flag = false
    for(c <- charArr) {
      c match {
        case '"' => {
          if(!flag) flag=true else flag=false
          // println(flag)
          }
        case ',' => {if(flag) sbf.append("") else sbf.append(",")}
        case _   => sbf.append(c)
      }
        
    }
    sbf.toString()
  }
  
  // This method checks whether a line has values surrounded by double quotes. And if so, whether it has all the starting and ending quotes
  // In data, it has been found that one line has been broken into multiple lines & hence all those lines can not be considered for evaluation
  // Ex. "wwbbbbwbwbb,fwbbbbwb // line# 1
  // wwbwbbbwbbwbb             // continuation of line# 1
  // ffqfqf, fqfqfqfqfq"       // continuation of line# 1
  // In this case line# 1 and it's continuations will be discarded. As obvious , Line#1 doesn't have even num of quotes ....
   def hasEvenNumQuotes(line : String) : Boolean = {
    val charArr = line.toCharArray()
    var cnt = 0 
    for(c <- charArr) {
      c match {
        case '"' => cnt = cnt + 1
        case _   => {}
      }  
    }
    if(cnt%2 == 0) true else false
  }
  
}