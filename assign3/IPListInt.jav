/*
 * Author: Gopidi Rajesh
 * File Name:  IPList.java
 * Course: COMP790-042
 * Assignment: #3
 *
 */
package HadoopProject.assign3;
import java.io.*;
import java.util.*;
import HadoopProject.LongArrayWritable;
import HadoopProject.IntArrayWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class IPList
{
    public static class IPMapper extends Mapper<LongWritable, Text, Text, IntArrayWritable> 
    {
        private Text intrMdtKey = new Text();
       
        @Override 
        public void map (LongWritable inputKey, Text inputValue, 
                         Context output) throws IOException, InterruptedException
        {
            byte parseCount = 0;
            final IntWritable zeroCount = new IntWritable(0);
            boolean isGreater = true;
            String ipAddr1 = null, temp = null;
            String ipAddr2 = null;
            int index = -1;
            IntWritable byteCount = new IntWritable(0);
            IntWritable[] iWArray = new IntWritable[2];
            IntArrayWritable array = new IntArrayWritable();
            String line = inputValue.toString();

            String[] tokens = line.split("\\s");
            for (String token: tokens) {
                if ((index = token.indexOf('.')) == -1) {
                    if (token.matches("\\d+")) {
                        byteCount.set(Integer.parseInt(token));
                        parseCount++;
                    } else if (token.equals("<")) {
                        isGreater = false;
                    }
                } else {
                    if (token.indexOf('.', index + 1) != -1) {
                        if (ipAddr1 == null) {
                            ipAddr1 = token.substring(0, token.lastIndexOf('.'));
                            parseCount++;
                        } else {
                            ipAddr2 = token.substring(0, token.lastIndexOf('.'));
                            parseCount++;
                        }
                    }
                }
                if (parseCount == 3)
                    break;
            }

            if (!isGreater) {
                temp = ipAddr1;
                ipAddr1 = ipAddr2;
                ipAddr2 = temp;
            }
            iWArray[0] = byteCount;
            iWArray[1] = zeroCount;
            array.set(iWArray);
            intrMdtKey.set(ipAddr1);
            output.write (intrMdtKey, array); 

            //dfsd
            iWArray[0] = zeroCount;
            iWArray[1] = byteCount;
            intrMdtKey.set(ipAddr2);
            array.set(iWArray);
            output.write (intrMdtKey, array);
        }
    } 

    public static class IPCountCombiner extends Reducer <Text, IntArrayWritable,
                                                        Text, IntArrayWritable>
    {
        @Override
        public void reduce (Text inputKey, Iterable<IntArrayWritable> values, Context output)
                            throws IOException, InterruptedException
        {
            int aduSent = 0;
            int aduRcvd = 0;
            Writable[] array = null;
            IntWritable[] iWArray = new IntWritable[2];
            IntArrayWritable intArrayW = new IntArrayWritable();
	    IntWritable temp = null;

            for (ArrayWritable value: values) {
                array = value.get();
                temp  = (IntWritable) array[0];
		aduSent += temp.get();
                temp  = (IntWritable) array[1];
                aduRcvd += temp.get(); 
            }
            iWArray[0] = new IntWritable(aduSent);
            iWArray[1] = new IntWritable(aduRcvd);
            intArrayW.set(iWArray);
            output.write(inputKey, intArrayW);
        }
    }


    public static class IPCountReducer extends Reducer <Text, IntArrayWritable,
                                                        Text, IntArrayWritable>
    {
        @Override
        public void reduce (Text inputKey, Iterable<IntArrayWritable> values, Context output)
                            throws IOException, InterruptedException
        {
            int aduSent = 0;
            int aduRcvd = 0;
            Writable[] array = null;
            IntWritable[] iWArray = new IntWritable[2];
            IntArrayWritable intArrayW  = new IntArrayWritable();
            IntWritable temp = null;

            for (ArrayWritable value: values) {
                array = value.get();
                temp  = (IntWritable) array[0];
                aduSent += temp.get();
                temp  = (IntWritable) array[1];
                aduRcvd += temp.get();  
            }

            iWArray[0] = new IntWritable(aduSent);
            iWArray[1] = new IntWritable(aduRcvd);
            intArrayW.set(iWArray);
            output.write(inputKey, intArrayW);
        }
    }
}
