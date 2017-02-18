package ADU2;// times each unique IP address 4-tuple appears in an
// adudump file.

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HostBytes {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: <input path> <output path>");
            System.exit(-1);
        }
        //define the job to the JobTracker

        System.out.println("Input Path :"+args[0]);
        System.out.println("Output Path :"+args[1]);

        Configuration conf = new Configuration();

        Job job = new Job(conf,"Host Count");
        job.setJarByClass(HostBytes.class);
        job.setJobName("Host Counts");

        // set the input and output paths (passed as args)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // set the Mapper and Reducer classes to be called
        job.setMapperClass(HostBytesMapper.class);
        job.setReducerClass(HostBytesReducer.class);

        // set the format of the keys and values
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set the number of reduce tasks
//        job.setNumReduceTasks(10);

        // submit the job and wait for its completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

    /*
    It is probably sufficient to verify the following for each input line (assume fields are properly delimited by blanks):
        - there are at least six fields.
        - the first field contains "ADU:"
        - the third and fifth fields contain exactly 4 instances of "."
        - the fourth field contains either "<" or ">"
        - the sixth field contains the string representation of an integer value.
        If any of these tests fail, ignore that line

                */