package ADU_SortSend;// times each unique IP address 4-tuple appears in an
// adudump file.

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HostSortSendReducer
        extends Reducer<LongWritable, Text, LongWritable, Text> {
    @Override
    public void reduce(LongWritable key, Iterable<Text> values,
                       Context context)
            throws IOException, InterruptedException {

        long count = 0;
        // iterate through all the values (count == 1) with a common key
        for (Text value : values) {


            context.write(null, new Text(value));
        }

    }
}
