package ecp.bigdata.PageRank;

 
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;



public class Map1 extends Mapper<LongWritable, Text, Text, Text> {


	 
@Override
public void map(LongWritable key, Text val, Context context) throws IOException,InterruptedException{
    
	
	 String[] pair = val.toString().split("\\s+");
     context.write(new Text(pair[0]), new Text(pair[1]));


    }
}

