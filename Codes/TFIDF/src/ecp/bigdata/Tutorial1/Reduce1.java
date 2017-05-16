package ecp.bigdata.Tutorial1;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;





public class Reduce1 extends Reducer<Text, IntWritable, Text, Text> {

    

    @Override

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException

    {

        int sum = 0;
        for (IntWritable val : values) {
           sum += val.get();
        }
        
        context.write(key, new Text(Integer.toString(sum)));

    }

}
