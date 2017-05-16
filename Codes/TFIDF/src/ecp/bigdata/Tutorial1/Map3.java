package ecp.bigdata.Tutorial1;


import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;



public class Map3 extends Mapper<Text, Text, Text, Text> {



@Override
public void map(Text key, Text val, Context context) throws IOException,InterruptedException{
    
	 String[] word_doc = key.toString().split("@");
	 String[] counts = val.toString().split("@");
	 
     context.write(new Text(word_doc[0]), new Text(word_doc[1]+"@"+counts[0]+"@"+counts[1]));
     
    }
}
