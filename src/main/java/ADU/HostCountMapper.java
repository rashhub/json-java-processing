package ADU;

import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;

public class HostCountMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\\s");
        String IPaddr1 = new String();
        String IPaddr2 = new String();
        int last_dot;
        // get the two IP address.port fields
        IPaddr1 = tokens[2];
        IPaddr2 = tokens[4];

        // eliminate the port part
        last_dot = IPaddr1.lastIndexOf('.');
        IPaddr1 = IPaddr1.substring(0, last_dot);
        last_dot = IPaddr2.lastIndexOf('.');
        IPaddr2 = IPaddr2.substring(0, last_dot);

        // output the key, value pairs where the key is an
        // IP address 4-tuple and the value is 1 (count)
        context.write(new Text(IPaddr1), new IntWritable(1));
        context.write(new Text(IPaddr2), new IntWritable(1));
    }
}

