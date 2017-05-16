package ecp.bigdata.PageRank;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;





public class Reduce1 extends Reducer<Text, Text, Text, Text> {

    

    @Override

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException

    {
    	StringBuilder res = new StringBuilder();
    	for (Text val : values){
    		res.append(val.toString());
    		res.append(";");
    	}

        context.write(key, new Text("1@"+res.toString()));
        

    }

}
