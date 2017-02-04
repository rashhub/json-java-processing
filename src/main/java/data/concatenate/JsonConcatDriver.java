package data.concatenate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.net.URI;


/**
 * Created by Rash on 21-10-2016.
 */
public class JsonConcatDriver {

    public static void main(String[] args)
            throws Exception
    {
        Configuration conf = new Configuration();
        //String uri ="hdfs://comet-21-67.ibnet:54310/";
     //   String uri = conf.get("fs.default.name");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2)
        {
            System.err.println("Incorrect Usage");
            System.exit(2);
        }

        System.out.println("Input Path :"+otherArgs[0]);
        System.out.println("Output Path :"+otherArgs[1]);


       // String hdfs_path=uri + File.separator+otherArgs[0];

       // FileSystem fs= FileSystem.get(new URI(uri),conf);
        //FileStatus[] fileStatus = fs.listStatus(new Path(hdfs_path));


        Job job = new Job(conf, "Concatenate JSON");
        job.setJarByClass(JsonConcatDriver.class);
        job.setMapperClass(JsonConcatMapper.class);
       // job.setReducerClass(JsonConcatReducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //Read files from multiple subfolder
      /*  for (int i = 0; i < otherArgs.length - 1; i++) {

            System.out.println("Inside otherargs 1");

            File _file=new File(otherArgs[0]);
            System.out.println("Inside otherargs 2");
            String [] dir=_file.list();
            System.out.println("Inside otherargs 3");

            for(int p=0;p<dir.length;p++) {
                System.out.println("directories :" + dir[p]);
            } */

        /*for(FileStatus _dfs : fileStatus)
        {

            System.out.println(_dfs.getPath().toString());

            //FileInputFormat.addInputPath(job, new Path(otherArgs[i]+File.separator+dir[j]));
        }*/


            //for(FileStatus dfs : fileStatus)
            //{

             //   FileInputFormat.addInputPath(job, new Path(dfs.getPath().toString()));

               FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
           //}


        //}
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[(otherArgs.length - 1)]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}


