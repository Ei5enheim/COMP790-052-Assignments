/*
 * Author: Gopidi Rajesh
 * File Name:  IPListReducer.java
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

public class IPListReducer
{

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
            context.write(inputKey, new LongWritable(count));
        }
    }
}
