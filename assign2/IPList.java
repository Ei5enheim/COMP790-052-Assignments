/*
 * Author: Gopidi Rajesh
 * File Name:  IPList.java
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

public class IPList
{

    public static class IPMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
    {
        private final static IntWritable count = new IntWritable(1);
        private Text intrMdtKey = new Text();
       
        @Override 
        public void map (LongWritable inputKey, Text inputValue, 
                         Context output) throws IOException, InterruptedException
        {
            String line = inputValue.toString();
            String ipAddr = null;
            String[] tokens = line.split("\\s");

            if (line.indexOf('>') != -1) {
                ipAddr = tokens[2];
                ipAddr = ipAddr.substring(0, ipAddr.lastIndexOf('.'));
            } else {
                ipAddr = tokens[4];
                ipAddr = ipAddr.substring(0, ipAddr.lastIndexOf('.'));
            }
            intrMdtKey.set(ipAddr);
            output.write (intrMdtKey, count); 
        }
    } 

    public static class IPCountCombiner extends Reducer <Text, LongWritable,
                                                        Text, LongWritable>
    {
        @Override
        public void reduce (Text inputKey, Iterable<LongWritable> values, Context output)
                            throws IOException, InterruptedException
        {
            long count = 0;

            for (LongWritable value: values) {
                count += value.get();
            }
            output.write(inputKey, new LongWritable(count));
        }
    }


    public static class IPCountReducer extends Reducer <Text, IntWritable,
                                                        Text, LongWritable>
    {
        @Override
        public void reduce (Text inputKey, Iterable<IntWritable> values, Context output)
                            throws IOException, InterruptedException
        {
            long count = 0;

            for (IntWritable value: values) {
                count += value.get();
            }
            output.write(inputKey, new LongWritable(count));
        }
    }

}
