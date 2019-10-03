// Use the named values (val) below whenever your need to
// read/write inputs and outputs in your program. 
val inputFilePath  = "./sample_input.csv"
val outputDirPath = "./output"


// Write your solution here
import java.io.PrintWriter
val file = sc.textFile(inputFilePath, 1)                                           // load file according to inputFilePath to RDD
val words = file.flatMap(line => line.split("\n"))                                 // split whole text by \n
val pairs = words.map(x => ((x.split(",")(0)), x.split(",")(3)))                   // filter the first item and last item, eg. http://subdom0001.example.com,/endpoint0001,GET,3B => http://subdom0001.example.com,3B
val combo = pairs.reduceByKey(_ + " " + _)                                         // reduce item with same key
var comboLs = combo.take(combo.count.toInt)                                        // take all item to form a list
var resultMap = scala.collection.mutable.Map[String, String]()                     // new a Map[String, String]

for (i <- 1 to comboLs.size) {
    val item = comboLs(i-1)._2.split(" ")                                          // split every item ini comboLs by 
    var list = List[Long]()                                                        // new a list of long
 
    if (item.size != 0){
        for (j <- item) {                                                          // convert KB MB into B
            if (j.contains("KB"))                                                  // when word contain KB
                list = list :+ j.replace("KB", "").toLong * 1024
            else if (j.contains("MB"))                                             // when word contain MB
                list = list :+ j.replace("MB", "").toLong * 1024 * 1024
            else                                                                   // when word contain B
                list = list :+ j.replace("B", "").toLong
        }

        val avg = list.sum / list.size                                             // calculate the average
        var r = list.map((_ - avg)).map(x => x * x)                                // calculate the varience
        var s = list.max.toString + "B," + list.min.toString + "B," + (list.sum / list.size).toString + "B," + (r.sum / r.size).toString + "B"    // form value to the output format
       
        resultMap += (comboLs(i-1)._1 -> s)                                        // add key-value pair into map
    }
    
    if (i == comboLs.size) {                                                       // output before end loop
        val mapSortSmall = resultMap.toList.sortBy(_._1).map({ case (x, y) => x + "," + y})
        val result = sc.makeRDD(mapSortSmall)                                      // store the mapSortSmall to the RDD
        result.coalesce(1,true).saveAsTextFile(outputDirPath)                      // save RDD status
    }
}
