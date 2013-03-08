
/*
 * Author: Gopidi Rajesh
 * File Name: LongArrayWritable.java
 * Course: COMP790-042
 * Assignment: #3
 *
 */

package HadoopProject;

import org.apache.hadoop.io.*;

public class LongArrayWritable extends ArrayWritable
{

    public LongArrayWritable()
    {
        super(LongWritable.class);
    }

    @Override
    public String toString()
    {
        StringBuilder str = new StringBuilder();
        for (String s: super.toStrings()) {
            str.append(s).append(" ");
        }
        return (str.toString());
    }

}


