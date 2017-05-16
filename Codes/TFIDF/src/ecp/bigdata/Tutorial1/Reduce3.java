package ecp.bigdata.Tutorial1;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;




public class Reduce3 extends Reducer<Text, Text,  Text,DoubleWritable> {

    

    @Override

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException

    {
    	// Compute Docperwords 
    	
    	int docperwords = 0;
    	ArrayList<String> cache_values = new ArrayList<String>();
    	
    	for (Text val : values){
    		cache_values.add(val.toString());
    		docperwords+=1;
    	
    	}
    	
    	
    	
    	int size = cache_values.size();
    	
    	for (int j = 0; j < size; ++j) {
    		
    		String[] doc_wc_wpd = cache_values.get(j).split("@");
    		// compute tfidf 
    		double tf = (double) Integer.parseInt(doc_wc_wpd[1])/Integer.parseInt(doc_wc_wpd[2]);
    		double idf = Math.log(2/docperwords);
    		double tfidf = tf*idf;
    		
    		context.write(new Text(key.toString()+"@"+doc_wc_wpd[0]),new DoubleWritable(tfidf));
    		
    	}



    }

}
