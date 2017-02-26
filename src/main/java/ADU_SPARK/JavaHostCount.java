package ADU_SPARK;
// Spark implementation of job to count the number of times each
// unique IP address 4-tuple appears in an adudump file.
//


import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;
import java.io.*;

public final class JavaHostCount {

    // The argument to the main function is the input file name
    // (specified as a parameter to the spark-submit command)
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaHostCount <file>");
            System.exit(1);
        }

        // Create a new Spark Context
        SparkConf conf = new SparkConf().setAppName("ADUReceived");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create a JavaRDD of strings; each string is a line read from
        // a text file.
        JavaRDD<String> lines = sc.textFile(args[0]);


        // The flatMap operation applies the provided function to each
        // element in the input RDD and produces a new RDD consisting of
        // the strings returned from each invocation of the function.
        // The strings are returned to flatMap as an iterator over a
        // list of strings (string array to string list with iterator).

        //Create Pair RDD of Receiver IP address and Count 1
        JavaPairRDD<String,Integer> receiver_count  = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] tokens = s.split(" ");
                String IPaddr1 = new String();
                String IPaddr2 = new String();

                int last_dot;

                // get the two IP address.port fields
                IPaddr1 = tokens[2];
                IPaddr2 = tokens[4];

                // eliminate the port part
                last_dot = IPaddr1.lastIndexOf('.');
                IPaddr1 = IPaddr1.substring(0, last_dot);
                last_dot = IPaddr2.lastIndexOf('.');
                IPaddr2 = IPaddr2.substring(0, last_dot);

                //if > reveiver is IP2, else if < receiver is IP1
                String Key =  tokens[3].equals(">")?IPaddr2:IPaddr1; //sort -k 2 part-r-00000 > checkfile

                return new Tuple2<String, Integer>(Key,1); //Return receiver IP Address with count 1

            }
        });


        //Calculate aggregated count for receiver ip addresses.
        JavaPairRDD<String, Integer> receiver_agg_counts = receiver_count.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });


        //Swap the RDD (key -> count , Value -> IP Address) and sort by Key.

        JavaPairRDD<Integer,String> swapped_sorted_rdd = receiver_agg_counts.mapToPair
                (new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<Integer, String>(stringIntegerTuple2._2(),stringIntegerTuple2._1());
            }
        });

         swapped_sorted_rdd.sortByKey(false);



        //Java to redirect stdout to a named file
        System.setOut(new PrintStream(new FileOutputStream("reveivedhosts")));
        //collect all the reduced <K,V> pairs as a list of Tuple2 objects
        //and output each as a line in the file
        List<Tuple2<Integer, String>> output = swapped_sorted_rdd.collect();
        for (Tuple2<?,?> tuple : output) {
            //the Tuple2 methods ._1() and ._2() get the pair 1st and 2nd values
            System.out.println(tuple._2() + ": " + tuple._1()); //Print IP address and Value(ADU count)
        }

        sc.stop();
    }
}
