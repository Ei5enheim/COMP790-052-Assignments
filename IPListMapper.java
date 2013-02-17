/*
 * Author: Gopidi Rajesh
 * File Name:  IPListMapper.java
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

public class IPListMapper
{

    public static class IPMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
    {
        private final static IntWritable count = new IntWritable(1);
        private Text intrMdtKey = new Text();
       
        @override 
        public void map (LongWritable inputKey, Text inputValue, 
                         Context output) throws IOException, InterruptedException
        {
            String line = inputValue.toString();
            String ipAddr = null;
            String[] tokens = line.split("\\s");

            if (line.indexOf('>') != -1) {
                ipAddr = token[2];
                ipAddr = ipAddr.substring(0, ipAddr.lastIndexOf('.'));
            } else {
                ipAddr = token[4];
                ipAddr = ipAddr.substring(0, ipAddr.lastIndexOf('.'));
            }
            intrMdtKey.set(ipAddr);
            output.write (intrMdtKey, count); 
        }
    } 
}
