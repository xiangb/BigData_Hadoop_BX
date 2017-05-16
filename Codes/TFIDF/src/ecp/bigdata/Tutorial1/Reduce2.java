package ecp.bigdata.Tutorial1;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;




public class Reduce2 extends Reducer<Text, Text, Text, Text> {

    

    @Override

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException

    {
    	
    	int nbdocwords = 0;
    	ArrayList<String> cache_values = new ArrayList<String>();
    	
    	for (Text val : values){
    		cache_values.add(val.toString());

    		String[] word_count = val.toString().split("=");
    		nbdocwords += Integer.parseInt(word_count[1]);
    	
    	}

    	int size = cache_values.size();
    	
    	for (int j = 0; j < size; ++j) {
    		
    		String[] word_count = cache_values.get(j).split("=");
    		context.write(new Text(word_count[0]+"@"+key.toString()), new Text(word_count[1]+"@"+Integer.toString(nbdocwords)));
    		
    	}



    }

}
