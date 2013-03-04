/*
 * Author: Gopidi Rajesh
 * File Name:  IPListTest.java
 * Course: COMP790-042
 * Assignment: #2
 *
 */

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class IPListTest
{
    
    public static void main (String args[]) throws Exception
    {
        
        long startTime = 0, finishTime = 0;
        boolean exitStatus = false;

        if (args.length < 2) {
            System.err.println("In sufficient arguments \n");
            System.exit(-1);
        } 
        Job job = new Job();

        job.setJarByClass(IPListTest.class);
        job.setJobName("Retrieval of ADU count");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(IPList.IPMapper.class);
        job.setReducerClass(IPList.IPCountReducer.class);
	//job.setCombinerClass(IPList.IPCountCombiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
	job.setNumReduceTasks(10);
	System.out.println("********** Starting the job now *********");
        //job.setNumReduceTasks(1.75 * nodes * mapred.tasktracker.reduce.tasks.maximum);
        startTime = new Date().getTime();
        exitStatus = job.waitForCompletion(true);
        finishTime = new Date().getTime();
        System.out.println("%%%%%%%%%%%%%%%% Begining $$$$$$$$$$$$$$$$$$");
        System.out.println("Time elapsed : "+ (finishTime - startTime));
        System.out.println("%%%%%%%%%%%%%%%% The END $$$$$$$$$$$$$$$$$$");
        System.exit(exitStatus ? 0 : 1); 
    }
}
