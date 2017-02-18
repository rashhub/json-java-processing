package ADU2;// times each unique IP address 4-tuple appears in an
// adudump file.

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HostBytesReducer
        extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values,
                       Context context)
            throws IOException, InterruptedException {

        long sentcount = 0;
        long receivedcount = 0;
        // iterate through all the values (count == 1) with a common key
        for (Text value : values) {

            String val = value.toString();

            String [] byte_info = val.split(" ");

            sentcount += Long.valueOf(byte_info[0]);

            receivedcount += Long.valueOf(byte_info[1]);

        }

        String outValue = Long.toString(sentcount).concat("\t").concat(Long.toString(receivedcount));

        context.write(key, new Text(outValue));
    }
}
