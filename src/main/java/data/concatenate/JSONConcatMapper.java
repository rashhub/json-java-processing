package data.concatenate;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.util.StringTokenizer;

//import org.apache.directory.api.util.exception.Exceptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by Rash on 16-10-2016.
 */
public class JsonConcatMapper extends Mapper<Object, Text,Text,NullWritable>{

    public void map(Object key,Text Value,Context context) throws IOException, InterruptedException
    {
        String line=Value.toString();
        context.write(new Text(line),NullWritable.get() );

    }

}
