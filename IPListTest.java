/*
 * Author: Gopidi Rajesh
 * File Name:  IPListTest.java
 * Course: COMP790-042
 * Assignment: #2
 *
 */

package Hadoop-project;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class IPListTest
{
    
    public static void main (String args[])
    {
        
        long startTime = 0, endTime = 0;
        boolean exitStatus = false;

        if (args.length < 2) {
            System.err.println("In sufficient arguments \n");
            System.out.exit(-1);
        } 
        Job jobConf = new Job();

        job.setJarByClass(IPListTest.class);
        job.setJobName("Retrieval of ADU count");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(IPMapper.class);
        job.SetReducerClass(IPCountReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapInputKeyClass(IntWritable.class);
        job.setMapInputValueClass(Text.class);
        
        job.setNumReduceTasks(10);
        startTime = job.getStartTime();
        exitStatus = job.waitForCompletion(true);
        endTime = job.getEndTime();
        System.out.println("%%%%%%%%%%%%%%%% Begining $$$$$$$$$$$$$$$$$$");
        System.out.println("Time elapsed : "+ endTime - startTime);
        System.out.println("%%%%%%%%%%%%%%%% The END $$$$$$$$$$$$$$$$$$");
        System.exit(exitStatus); 
    }
}
