package assignment1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * This class solves the problem posed for Assignment1
 * among that use the class Ngramiterator to implements ngrams algorithm
 *
 */
public class Assignment1 {
	private static int ngrams;            // The value N for the ngram
	private static int times;             // The minimum count for an ngram to be included in the output file. 
	private static NgramIterator ni;
	
// TODO: Write the source code for your solution here.	
	public static class NgramIterator implements Iterator<String> {
		private final List<String> Ls;                            // ArrayList used to stored words
	    private final int n;                                      // variable for n grams
	    int pos = 0;                                              // position
	    
	    public NgramIterator(int n, String str) {                 // generator method
	        this.n = n;
	        this.Ls = new ArrayList();
	        
	        StringTokenizer itr = new StringTokenizer(str);       // divide sentences into words based on spaces
		    while (itr.hasMoreTokens()) {
		       this.Ls.add(itr.nextToken());
		    }
	    }
	    
	    public boolean hasNext() {                                // override hasNext method, determine whether have next item
	        return pos < Ls.size() - n + 1;
	    }
	    
	    public String next() {                                    // override next method, return next N words sentences
	        String result = "";
	        int origin = pos; 
	        for (int i = pos; i < origin + n; i ++) {
	        	result += Ls.get(i);
	        	if (i != origin + n - 1)
	        		result += " ";
	        }
	        pos ++;
	        return result;
	    }
	}
	
    // Mapper function implement here
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
	   private Text packet = new Text();    // output value
	   private Text word = new Text();      // output key
	
	   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		   String str = value.toString();                  
		   InputSplit inputSplit =context.getInputSplit();            // get belong file
		   String fileName = ((FileSplit)inputSplit).getPath().getName();
		  
		   packet.set(fileName);                                      // Type change: String -> Text
		   
		   ni = new NgramIterator(ngrams, str);                       // implement the NgramIterator algorithms in NgramIterator class
		   while (ni.hasNext()) {                                     // write the key-value pair into context
			   word.set(ni.next());
			   context.write(word, packet);
		   }
	   }
	 }
  
  // Reducer function implement here
  	public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
	   private Text result = new Text();             // output value
	   private String file_exist;                    // String used for connect all words in set
	 
	   public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		     int sum = 0;                            // total appearance time
		     HashSet set = new HashSet<String>();    // Hashset used to avoid duplication
		     
		     for (Text val : values) {               // put input values from list to set and calculate its size
		    	 set.add(val.toString());
		    	 sum += 1;
		     }
		                   
		     Iterator setIter = set.iterator();      // Splice all the characters in the set
		     file_exist = "";
		     while (setIter.hasNext()) {
		    	 file_exist += setIter.next();
		    	 if (setIter.hasNext())
		    		 file_exist += " ";
		     }
		     result.set(String.valueOf(sum) + "  " + file_exist);
		     
		     if (sum >= times)                      // determine threshold then output key-value pair 
		    	 context.write(key, result);
	   }
	}
	
	// main methods here
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();                    //Creating a Configuration object and a Job object, assigning a job name for identification purposes
	    Job job = Job.getInstance(conf, "asst1");                    
	    job.setJarByClass(Assignment1.class);                        //Setting the job's jar file by finding the provided class location
	    
	    job.setMapperClass(TokenizerMapper.class);                   //Providing the mapper and reducer class names
	    job.setReducerClass(IntSumReducer.class);
	    
	    job.setOutputKeyClass(Text.class);                           //Setting configuration object with the Data Type of output Key and Value for map and reduce
	    job.setOutputValueClass(Text.class);
	    
	    ngrams = Integer.valueOf(args[0]);                           // get parameters
	    times = Integer.valueOf(args[1]);
	    FileInputFormat.addInputPath(job, new Path(args[2]));
	    FileOutputFormat.setOutputPath(job, new Path(args[3]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	
}
