package ADU_SortReceive;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HostSortReceiveMapper
        extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\t");

        Long receiverBytestoSort = Long.valueOf(tokens[2]);

        String valueText = value.toString();


        // IP address 4-tuple and the value is 1 (count)
        context.write(new LongWritable(receiverBytestoSort), new Text(value));
        //context.write(new Text(IPaddr2), new IntWritable(1));
    }
}

