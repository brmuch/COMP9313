import scalaj.http._
import scala.util.parsing.json._
import scala.xml._
import sys.process._
import java.io.File
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.Set

object CaseIndex {
    def main(args: Array[String]) {
        val inputFileDir = args(0)                                              // get input File Dir from args        
        def getFile(file:File): Array[File] ={                                  // load .xml file here
            val files = file.listFiles().filter(! _.isDirectory)                // filter all xml in given directory
                .filter(t => t.toString.endsWith(".xml"))   
                files ++ file.listFiles().filter(_.isDirectory).flatMap(getFile)
        }

        val path = new File("./cases_test")                                      // get ./xml directory
        val index = Http("http://localhost:9200/legal_idx?pretty").method("PUT").header("Content-Type", "application/json").asString         // create new index
        
        val jsonData = "{\"cases\": {\"properties\": {\"name\": {\"type\":\"text\"},\"url\": {\"type\": \"text\"},\"location\": {\"type\": \"text\"}, 
                        \"person\": {\"type\": \"text\"},\"organiztion\": {\"type\": \"text\"},\"catchphrases\": {\"type\": \"text\"}, \"sentences\": {\"type\": \"text\"}}}}"             // define json of mapping
        val result = Http("http://localhost:9200/legal_idx/cases/_mapping?pretty").method("PUT").header("Content-Type", "application/json").postData(jsonData).asString                    // create new mapping and type with Http request
        println(result)
        
        for (fileName <- getFile(path)) {                                                                                // handle each .xml file
            val xml = XML.loadFile(fileName)                                                                             // load .xml file by filename
            var name = (xml\"name").text                                                                                 // get content of .xml file
            
            var catchphrases = ((xml\"catchphrases").text).trim().replace("\n", " ")                                     // parse out catchphrases from content of .xml file and processing the text
            var sentences = ((xml\"sentences").text).trim().replace("\n", " ")                                           // parse out sentences from content of .xml file and processing the text
            var link = (xml\"AustLII").text                                                                              // parse out url link

            val together = ((xml\"catchphrases").text).trim() + "\n" + ((xml\"sentences").text).trim()                   // put catchphrases and sentences together
            val phaseLs = together.split("\n")                                                                           // split into single sentence by \n
            
            var locationSet : Set[String] = Set()                                                                        // locationSet to store LOCATION entities
            var personSet : Set[String] = Set()                                                                          // personSet to store PERSON entities
            var organizationSet : Set[String] = Set()                                                                    // organizationSet to store ORGANIZATION entities

            // analysis catchphrases and sentences
            for (phase <- phaseLs) {            
                if (phase != "" && phase != " ") {                  
                    val response = Http("http://localhost:9000/?properties=%7B'annotators':'ner','outputFormat':'json'%7D").postData(phase)
                                    .method("POST").header("Content-Type", "appslication/json").option(HttpOptions.connTimeout(60000)).option(HttpOptions.readTimeout(60000)).asString.body             // post data to stanford corenlp server by http request
                    val b = JSON.parseFull(response)                                                                                        // handle Json response
                    val jsonObj = b match {case Some(map:Map[String, List[Any]]) => map.get("sentences")}                                   // get attribute of key(sentences) in Map
                    val jsonObj1 = jsonObj match {case Some(list:List[Map[String, Any]]) => list(0).get("tokens")}                          // get attribute of key(tokens) in Map from a List
                    val jsonObj2 = jsonObj1 match {case Some(list:List[Map[String, String]]) => list}                                       // get List which is a list of Map
                    
                    val jsonObj3 = jsonObj2.asInstanceOf[List[Any]]                                                                         // predefine jsonObj3 is a List
                    for (elem <- jsonObj3) {                                                                                                // Traversing all element of jsonObj3
                        var temp = (elem.asInstanceOf[Map[String, String]]).get("ner")                                                      // parse out ner attribute
                        var originalTemp = (elem.asInstanceOf[Map[String, String]]).get("originalText")                                     // parse out originalText attribute

                        var types = temp match {case Some(str1: String) => str1}                                                            // turn originalText into String
                        var originalTx = originalTemp match {case Some(str1: String) => str1}
                        
                        // determine here
                        if (types == "LOCATION") {                                                                                          // if types is LOCATION
                            locationSet += originalTx.asInstanceOf[String]                                                                  // put into locationSet
                        }
                        else if (types == "PERSON") {                                                                                       // if types is PERSON
                            personSet += originalTx.asInstanceOf[String]                                                                    // put into personSet
                        }
                        else if (types == "ORGANIZATION"){                                                                                  // if types is ORGANIZATION
                            organizationSet += originalTx.asInstanceOf[String]                                                              // put into organizationSet
                        }
                    }
                }
            }

            name = name.replace(" ", "%20")                                                                                                 // replace space with %20
            val httpBase = "http://localhost:9200/legal_idx/cases/" + name + "?pretty"                                                      // 

            name = name.replace("%20", " ")                                                                                                 // replace %20 with space
            name = name.replace("'", "\'").replace("\"", "\\\"")                                                                            // Escape "  and '  in the content
            link = link.replace("'", "\'").replace("\"", "\\\"")                                                                            // Escape "  and '  in the content
            val jsonContent = "{\"name\":\"" + name + "\",\"url\":\"" + link + "\",\"location\":\"" + locationSet.mkString(" ").replace("'", "\'")
                            .replace("\"", "\\\"") + "\",\"person\":\"" + personSet.mkString(" ").replace("'", "\'").replace("\"", "\\\"") 
                            + "\",\"organization\":\"" + organizationSet.mkString(" ").replace("'", "\'").replace("\"", "\\\"") + "\",\"catchphrases\":\"" 
                            + catchphrases.replace("'", "\'").replace("\"", "\\\"") + "\",\"sentences\":\"" + sentences.replace("'", "\'").replace("\"", "\\\"") + "\"}" // Json Put data into index
            val addResponse = Http(httpBase).method("PUT").header("Content-Type", "application/json").postData(jsonContent).asString.body   // new a document with Http request
            println(addResponse)                                                                                                            // print http response
        }
    }
}
