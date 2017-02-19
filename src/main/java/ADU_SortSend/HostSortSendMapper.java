package ADU_SortSend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HostSortSendMapper
        extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\t");

        Long senderBytestoSort = Long.valueOf(tokens[1]);

        String valueText = value.toString();


        // IP address 4-tuple and the value is 1 (count)
        context.write(new LongWritable(senderBytestoSort), new Text(value));
        //context.write(new Text(IPaddr2), new IntWritable(1));
    }
}

