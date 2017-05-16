package ecp.bigdata.PageRank;


import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;



public class Map2 extends Mapper<Text, Text, Text, Text> {



@Override
public void map(Text key, Text val, Context context) throws IOException,InterruptedException{
    
	// count number of outgoing link for key 
	// start at -1 to avoid to count 1 
	int nblinks = 0; 
	String [] links = val.toString().split("@");
	for (String page : links[1].split(";"))
	{
		nblinks+=1;
	}
	
    

	for (String page : links[1].split(";"))
	{	
		
			context.write(new Text(page), new Text(key.toString()+";"+links[0]+ ";"+Integer.toString(nblinks)));

	}
	 
     
     
    }
}
