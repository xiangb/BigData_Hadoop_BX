package ecp.bigdata.Tutorial1;

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

public class TFIDFMapReduce extends Configured implements Tool {


    public int run(String[] args) throws Exception {

        if (args.length != 4) {

            System.out.println("Usage: [input] [output]");

            System.exit(-1);

        }

        // We are creating a 3 chained jobs to compute tf-idf for the words
        // ##### First Job ##### //
        
        // Création d'un job en lui fournissant la configuration et une description textuelle de la tâche

        Job job = Job.getInstance(getConf());

        job.setJobName("TF-IDF-part1");


        // On précise les classes MyProgram, Map et Reduce

        job.setJarByClass(TFIDFMapReduce.class);
        job.setMapperClass(Map1.class);
        job.setReducerClass(Reduce1.class);


        // Définition des types clé/valeur de notre problème

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

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
        
        Job job1 = Job.getInstance(getConf());
        job1.setJobName("TF-IDF-part2");


        // On précise les classes MyProgram, Map et Reduce

        job1.setJarByClass(TFIDFMapReduce.class);
        job1.setMapperClass(Map2.class);
        job1.setReducerClass(Reduce2.class);


        // Définition des types clé/valeur de notre problème

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
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));


        //Suppression du fichier de sortie s'il existe déjà
        
        
        FileSystem fs1 = FileSystem.newInstance(getConf());
        if (fs1.exists(new Path(args[2]))) {

            fs1.delete(new Path(args[2]), true);
        }
        
        job1.waitForCompletion(true);
        
        // ##### Third job ##### //
        
        Job job2 = Job.getInstance(getConf());
        job2.setJobName("TF-IDF-part3");


        // On précise les classes MyProgram, Map et Reduce

        job2.setJarByClass(TFIDFMapReduce.class);
        job2.setMapperClass(Map3.class);
        job2.setReducerClass(Reduce3.class);


        // Définition des types clé/valeur de notre problème

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        
	    // set key-value separator for the input file
        job2.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        // set output separator
        job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
	    job2.setInputFormatClass(KeyValueTextInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);
	    


        // Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)
        // Output of first job is the input of the second job
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));


        //Suppression du fichier de sortie s'il existe déjà
        
        
        FileSystem fs2 = FileSystem.newInstance(getConf());
        if (fs2.exists(new Path(args[3]))) {

            fs2.delete(new Path(args[3]), true);
        }
        
        job2.waitForCompletion(true);
        
        
        return 0;
        

    }


    public static void main(String[] args) throws Exception {

        TFIDFMapReduce exempleDriver = new TFIDFMapReduce();

        int res = ToolRunner.run(exempleDriver, args);

        System.exit(res);

    }

}

