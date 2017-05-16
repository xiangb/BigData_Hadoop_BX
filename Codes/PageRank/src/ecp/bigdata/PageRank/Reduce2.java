package ecp.bigdata.PageRank;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.*;
import java.util.*;




public class Reduce2 extends Reducer<Text, Text, Text, Text> {

    
	private static final float damping = 0.85F;
    /* Initialise one time a hashmap to store key (line id) and the corresponding world*/   
    private static HashMap<String,String> key_links = new HashMap<String,String>();
   
 
    public Reduce2() throws NumberFormatException, IOException{
   
      /* Default constructor to store pair of line_id content */
     BufferedReader Reader_count = new BufferedReader(
               new FileReader(
                        new File(
                                "/home/xiangb/Cours3A/MSc/BigDataAlgorithms/Hadoop/Codes/PageRank/output1/part-r-00000"
                            )));
        
        String line;

        while ((line = Reader_count.readLine()) != null)
        {
            String[] parts = line.split("@", 2);
            if (parts.length >= 2)
            {
               
                key_links.put(parts[0].split(",",2)[0],parts[1]);
            
            } else {
                System.out.println("ignoring line: " + line);
            }
        }
        Reader_count.close();
        
        System.out.println(key_links.get("0"));
      }
    
     
     @Override
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{

    	double sum = 0;
    	StringBuilder res = new StringBuilder();
    	
    	for (Text val : values){
    		
    		String [] temp = val.toString().split(";");
    		sum += (double) Double.parseDouble(temp[1])/Integer.parseInt(temp[2]);
    		res.append(temp[0]);
    		res.append(";");
    		
    	}
    	
    	double newPG = (1-damping) + damping*sum;
    	
    	if (key_links.get(key.toString())!=null){
    	context.write(key, new Text(String.valueOf(newPG)+"@"+key_links.get(key.toString()).toString()));
    	}
  



    }

}
