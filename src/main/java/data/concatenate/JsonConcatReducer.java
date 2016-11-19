package data.concatenate;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Created by Rash on 22-10-2016.
 */
public class JsonConcatReducer extends Reducer<Text,NullWritable,Text,NullWritable>{

    public void reduce(Text key, NullWritable value, Mapper.Context context) throws IOException,InterruptedException
    {
        context.write(key,NullWritable.get());

    }

}
