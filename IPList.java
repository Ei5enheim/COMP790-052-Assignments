/*
 * Author: Gopidi Rajesh
 * File Name:  IPList.java
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

public class IPList
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

    public static class IPCountReducer extends Reducer <Text, IntWritable,
                                                        Text, LongWritable>
    {
        @override
        public void reduce (Text inputKey, Iterable<IntWritable> values, Context output)
                            throws IOException, InterruptedException
        {
            long count = 0;

            while (values.hasNext()) {
                count += values.next.get();
            }
            context.write(new LongWritable(count), inputKey);
        }
    }

}
