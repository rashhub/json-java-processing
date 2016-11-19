package data.concatenate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;

import java.io.File;


/**
 * Created by Rash on 21-10-2016.
 */
public class JsonConcatDriver {

    public static void main(String[] args)
            throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2)
        {
            System.err.println("Incorrect Usage");
            System.exit(2);
        }


        System.out.println("Input Path :"+otherArgs[0]);
        System.out.println("Output Path :"+otherArgs[1]);

        Job job = new Job(conf, "Concatenate JSON");
        job.setJarByClass(JsonConcatDriver.class);
        job.setMapperClass(JsonConcatMapper.class);
        job.setReducerClass(JsonConcatReducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //Read files from multiple subfolder
        for (int i = 0; i < otherArgs.length - 1; i++) {

            File _file=new File(otherArgs[0]);
            String [] dir=_file.list();

            for(int j=0;j<dir.length;j++)
            {
                FileInputFormat.addInputPath(job, new Path(otherArgs[i]+File.separator+dir[j]));
            }


        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[(otherArgs.length - 1)]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}


