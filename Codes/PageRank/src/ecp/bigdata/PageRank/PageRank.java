package ecp.bigdata.PageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;
import java.util.*;

public class PageRank extends Configured implements Tool {


    public int run(String[] args) throws Exception {

        if (args.length != 3) {

            System.out.println("Usage: [input] [output]");

            System.exit(-1);

        }

        // We are creating a 3 chained jobs to compute tf-idf for the words
        // ##### First Job ##### //
        
        // Création d'un job en lui fournissant la configuration et une description textuelle de la tâche

        Job job = Job.getInstance(getConf());

        job.setJobName("PageRank");


        // On précise les classes MyProgram, Map et Reduce

        job.setJarByClass(PageRank.class);
        job.setMapperClass(Map1.class);
        job.setReducerClass(Reduce1.class);


        // Définition des types clé/valeur de notre problème

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        /* set output as csv file */
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");

        // Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        //Suppression du fichier de sortie s'il existe déjà
        
        
        FileSystem fs = FileSystem.newInstance(getConf());

        if (fs.exists(new Path(args[1]))) {

            fs.delete(new Path(args[1]), true);
        }


        job.waitForCompletion(true);
        
        // ##### Second Job ##### // 
        
        // Repeat x times to compute pagerank
        int iterations = 40;
        Path inPath = new Path(args[1]);
        Path outPath =  null;
        for(int i=0;i<iterations;i++){
        Job job1 = Job.getInstance(getConf());
        outPath = new Path(args[2]+i);
        job1.setJobName("PageRank-part2");


  
        job1.setJarByClass(PageRank.class);
        job1.setMapperClass(Map2.class);
        job1.setReducerClass(Reduce2.class);


        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
	    // set key-value separator for the input file
        job1.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        // set output separator
        job1.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
	    job1.setInputFormatClass(KeyValueTextInputFormat.class);
	    job1.setOutputFormatClass(TextOutputFormat.class);
	    


        // Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)
        // Output of first job is the input of the second job
        FileInputFormat.addInputPath(job1, inPath);
        FileOutputFormat.setOutputPath(job1, outPath);
        

        //Suppression du fichier de sortie s'il existe déjà
        
        
        FileSystem fs1 = FileSystem.newInstance(getConf());
        if (fs1.exists(outPath)) {

            fs1.delete(outPath, true);
        }
        
        job1.waitForCompletion(true);
        inPath=outPath;
        }
        return 0;
        

    }


    public static void main(String[] args) throws Exception {

        PageRank exempleDriver = new PageRank();

        int res = ToolRunner.run(exempleDriver, args);

        System.exit(res);

    }

}

