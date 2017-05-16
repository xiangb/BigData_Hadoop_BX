package ecp.bigdata.Tutorial1;

 
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;



public class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {


	private final static IntWritable ONE = new IntWritable(1);
	 
@Override
public void map(LongWritable key, Text val, Context context) throws IOException,InterruptedException{
    
	 String doc = ((FileSplit) context.getInputSplit()).getPath().getName();
	 


     for (String token: val.toString().replaceAll("[^a-zA-Z0-9 ]", " ").split("\\s+")) {
    	
    	 context.write(new Text(token.toLowerCase()+"@"+doc), ONE);
     }

    }
}

