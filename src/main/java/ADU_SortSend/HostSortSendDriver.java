package ADU_SortSend;// times each unique IP address 4-tuple appears in an
// adudump file.

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HostSortSendDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: <input path> <output path>");
            System.exit(-1);
        }
        //define the job to the JobTracker

        System.out.println("Input Path :"+args[0]);
        System.out.println("Output Path :"+args[1]);

        Configuration conf = new Configuration();

        Job job = new Job(conf,"Host Sort");
        job.setJarByClass(HostSortSendDriver.class);
        job.setJobName("Host Sort");

        // set the input and output paths (passed as args)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // set the Mapper and Reducer classes to be called
        job.setMapperClass(HostSortSendMapper.class);
        job.setReducerClass(HostSortSendReducer.class);

        // set the format of the keys and values
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        // set the number of reduce tasks
//        job.setNumReduceTasks(10);

        // submit the job and wait for its completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
