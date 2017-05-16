package ecp.bigdata.Tutorial1;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class Dis_isdhistory {

	public static void main(String[] args) throws IOException {
		
		
		Path filename = new Path(args[0]);
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);
		
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			int count = 1;
			System.out.println("USAF   StationName                     FIPS   Altitude");
			while (line !=null){
				// skip first 22 lines
				if (count > 22){
				// Process of the current line
				String[] array = line.split("");
			
				for (int i = 0;i<=5;i++){
					System.out.printf(array[i]);
				}
				System.out.printf("\t");
				// Print Station Name
				for (int i = 13;i<=(13+29);i++){
					System.out.printf(array[i]);
				}
				
				System.out.printf("\t");
				
				for (int i = 43;i<=45;i++){
					System.out.printf(array[i]);
				}
				System.out.printf("\t");
				
				for (int i = 74;i<=81;i++){
					System.out.printf(array[i]);
				}
				
				System.out.printf("\n");
				

				
				// go to the next line
				
				}
				line = br.readLine();
				count +=1;
				
			}
		}
		finally{
			//close the file
			
			inStream.close();
			fs.close();
		}

		
		
	}

}
