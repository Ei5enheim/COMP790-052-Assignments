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
    public static class IPMapper extends Mapper<LongWritable, Text, Text, LongArrayWritable> 
    {
        private Text intrMdtKey = new Text();
        private static final LongWritable zeroCount = new LongWritable(0);

        @Override 
        public void map (LongWritable inputKey, Text inputValue, 
                         Context output) throws IOException, InterruptedException
        {
            byte parseCount = 0;
            boolean isGreater = true;
            String ipAddr1 = null, temp = null;
            String ipAddr2 = null;
            int index = -1;
            LongWritable byteCount = new LongWritable(0);
            LongWritable[] lWArray = new LongWritable[2];
            LongArrayWritable array = new LongArrayWritable();
            String line = inputValue.toString();

	    //splitts the text into tokens with space as delimiter 
            String[] tokens = line.split("\\s");
	    // iterates over the tokens
            for (String token: tokens) {
		// A check to see if the token contains '.' character
                if ((index = token.indexOf('.')) == -1) {
		    // using regex to check if the token is an integer literal
                    if (token.matches("\\d+")) {
                        byteCount.set(Long.parseLong(token));
                        parseCount++;
                    } else if (token.equals("<")) {
                        isGreater = false;
                    }
                } else {
		    // A check to see if the token is an IP addr
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
		// exits the loop as soon we parse both the ip addresses and adu value
                if (parseCount == 3)
                    break;
            }

	    // checks if the first IP address is the sender or the receiver
            if (!isGreater) {
		/* interchanging the IP addresses if the first IP address
		 * is the receiver.
		 */
                temp = ipAddr1;
                ipAddr1 = ipAddr2;
                ipAddr2 = temp;
            }
	    // Associating the number of bytes sent by a host with its IP address
            lWArray[0] = byteCount;
            lWArray[1] = zeroCount;
            array.set(lWArray);
            intrMdtKey.set(ipAddr1);
            output.write (intrMdtKey, array); 

            // Associating the number of bytes received by a host with its IP address
            lWArray[0] = zeroCount;
            lWArray[1] = byteCount;
            intrMdtKey.set(ipAddr2);
            array.set(lWArray);
            output.write (intrMdtKey, array);
        }
    } 

    public static class IPCountCombiner extends Reducer <Text, LongArrayWritable,
                                                        Text, LongArrayWritable>
    {
        @Override
        public void reduce (Text inputKey, Iterable<LongArrayWritable> values, Context output)
                            throws IOException, InterruptedException
        {
            long aduSent = 0;
            long aduRcvd = 0;
            Writable[] array = null;
            LongWritable[] lWArray = new LongWritable[2];
            LongArrayWritable longArrayW = new LongArrayWritable();
	    LongWritable temp = null;
	
	    /* Retrieves the number of bytes sent and 
	     * received by the IP address and accumulates them.
	     */
            for (ArrayWritable value: values) {
                array = value.get();
                temp  = (LongWritable) array[0];
		aduSent += temp.get();
                temp  = (LongWritable) array[1];
                aduRcvd += temp.get(); 
            }

	    // writes back the send and received counters
            lWArray[0] = new LongWritable(aduSent);
            lWArray[1] = new LongWritable(aduRcvd);
            longArrayW.set(lWArray);
            output.write(inputKey, longArrayW);
        }
    }


    public static class IPCountReducer extends Reducer <Text, LongArrayWritable,
                                                        Text, LongArrayWritable>
    {
        @Override
        public void reduce (Text inputKey, Iterable<LongArrayWritable> values, Context output)
                            throws IOException, InterruptedException
        {
            long aduSent = 0;
            long aduRcvd = 0;
            Writable[] array = null;
            LongWritable[] lWArray = new LongWritable[2];
            LongArrayWritable longArrayW  = new LongArrayWritable();
            LongWritable temp = null;

	    // iterating over all the key, value pairs of a particular key
            for (ArrayWritable value: values) {
                array = value.get();
                temp  = (LongWritable) array[0];
                aduSent += temp.get();
                temp  = (LongWritable) array[1];
                aduRcvd += temp.get();  
            }
	    //writes the aggregated sent and received counters to the output file.
            lWArray[0] = new LongWritable(aduSent);
            lWArray[1] = new LongWritable(aduRcvd);
            longArrayW.set(lWArray);
            output.write(inputKey, longArrayW);
        }
    }
}
