package ADU_SPARK;
// Spark implementation of job to count the number of times each
// unique IP address 4-tuple appears in an adudump file.
//


import org.apache.calcite.util.NumberUtil;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.catalyst.util.StringUtils;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;
import java.io.*;

public final class InteractiveHost {

    // The argument to the main function is the input file name
    // (specified as a parameter to the spark-submit command)
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: InteractiveHost <file>");
            System.exit(1);
        }

        // Create a new Spark Context
        SparkConf conf = new SparkConf().setAppName("InteractiveHost").setMaster("local");;
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
        JavaPairRDD<String, Integer> receiver_count_rdd = lines.mapToPair(new PairFunction<String, String, Integer>() {
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
                String Key = tokens[3].equals(">") ? IPaddr2 : IPaddr1; //sort -k 2 part-r-00000 > checkfile

                return new Tuple2<String, Integer>(Key, 1); //Return receiver IP Address with count 1

            }
        });


        //Create Pair RDD of Sender IP address and Count 1
        JavaPairRDD<String, Integer> sender_count_rdd = lines.mapToPair(new PairFunction<String, String, Integer>() {
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
                String Key = tokens[3].equals(">") ? IPaddr1 : IPaddr2; //sort -k 2 part-r-00000 > checkfile

                return new Tuple2<String, Integer>(Key, 1); //Return receiver IP Address with count 1

            }
        });


        //Calculate aggregated count for receiver ip addresses.
        JavaPairRDD<String, Integer> receiver_agg_counts = receiver_count_rdd.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        //Calculate aggregated count for sender ip addresses.
        JavaPairRDD<String, Integer> sender_agg_counts = sender_count_rdd.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        //Swap the RDD (key -> count , Value -> IP Address) and sort by Key.

        JavaPairRDD<Integer, String> swapped_receiver_sorted_rdd = receiver_agg_counts.mapToPair
                (new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return new Tuple2<Integer, String>(stringIntegerTuple2._2(), stringIntegerTuple2._1());
                    }
                }).sortByKey(false);


        JavaPairRDD<Integer, String> swapped_sender_sorted_rdd = sender_agg_counts.mapToPair
                (new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return new Tuple2<Integer, String>(stringIntegerTuple2._2(), stringIntegerTuple2._1());
                    }
                }).sortByKey(false);


        // Calculate Bytes for IP Addresses

        JavaPairRDD<String, Integer> receiver_byte_rdd = lines.mapToPair(new PairFunction<String, String, Integer>() {
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
                String Key = tokens[3].equals(">") ? IPaddr2 : IPaddr1; //sort -k 2 part-r-00000 > checkfile

                return new Tuple2<String, Integer>(Key, Integer.valueOf(tokens[5])); //Return receiver IP Address with count 1

            }
        });


        //Create Pair RDD of Sender IP address and Count 1
        JavaPairRDD<String, Integer> sender_byte_rdd = lines.mapToPair(new PairFunction<String, String, Integer>() {
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
                String Key = tokens[3].equals(">") ? IPaddr1 : IPaddr2; //sort -k 2 part-r-00000 > checkfile

                return new Tuple2<String, Integer>(Key, Integer.valueOf(tokens[5])); //Return receiver IP Address with count 1

            }
        });


        //Calculate aggregated count for receiver ip addresses.
        JavaPairRDD<String, Integer> receiver_agg_bytes = receiver_byte_rdd.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        //Calculate aggregated count for sender ip addresses.
        JavaPairRDD<String, Integer> sender_agg_bytes = sender_byte_rdd.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        //Swap the RDD (key -> count , Value -> IP Address) and sort by Key.

        JavaPairRDD<Integer, String> swapped_receiver_sorted_bytes_rdd = receiver_agg_bytes.mapToPair
                (new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return new Tuple2<Integer, String>(stringIntegerTuple2._2(), stringIntegerTuple2._1());
                    }
                }).sortByKey(false);


        JavaPairRDD<Integer, String> swapped_sender_sorted_bytes_rdd = sender_agg_bytes.mapToPair
                (new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return new Tuple2<Integer, String>(stringIntegerTuple2._2(), stringIntegerTuple2._1());
                    }
                }).sortByKey(false);



        Scanner scan = new Scanner(System.in);
        int input;

        //keep looping until user input 5 which is Exit
        do {
            System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));

                      System.out.println("1.Top Senders" +
                                         "\n2.Top Receivers " +
                                         "\n3.List of Senders with more than K bytes" +
                                         "\n4.List of Receivers with more than K bytes " +
                                         "\n5.IP Details (11.22.33.55)" +
                                         "\n6.Exit");
            //ask input from user
            System.out.println("Enter your choice: ");

            input = scan.nextInt();

            switch (input) {
                case 1:

                    System.out.println("\nPlease Note: File \"topsenderHosts\" will be generated in your current directory with results.");

                    System.out.println("\nPlease Enter Number of Top Senders to be listed");

                    int topK=scan.nextInt();

                    List<Tuple2<Integer,String>> topK_senders = swapped_sender_sorted_rdd.take(topK);

                        System.setOut(new PrintStream(new FileOutputStream("topSenderHosts")));

                        System.out.println("Below is the list of top ADU Senders:");

                        for (Tuple2<?,?> tuple : topK_senders) {

                            System.out.println(tuple._2() + ": " + tuple._1()); //Print IP address and Value(ADU count)
                        }

                    break;
                case 2:

                    System.out.println("\nPlease Note: File \"topreceiverHosts\" will be generated in your current directory with results.");

                    System.out.println("\nPlease Enter the Number of Top Receivers to be listed");


                    int topK_rec=scan.nextInt();

                    List<Tuple2<Integer,String>> topK_receivers = swapped_receiver_sorted_rdd.take(topK_rec);

                    System.setOut(new PrintStream(new FileOutputStream("topReceiverHosts")));

                    System.out.println("Below is the list of top ADU Receivers:");

                        for (Tuple2<?,?> tuple : topK_receivers) {

                            System.out.println(tuple._2() + ": " + tuple._1()); //Print IP address and Value(ADU count)
                        }

                    break;
                case 3:

                    System.out.println("\nPlease Note: File \"topSenderBytes\" will be generated in your current directory with results.");

                    System.out.println("\nPlease Enter the K-Bytes to get  Senders List.");


                    final int sender_bytes =scan.nextInt();

                    List<Tuple2<Integer,String>> senders = swapped_sender_sorted_bytes_rdd.filter(new Function<Tuple2<Integer, String>, Boolean>() {
                        @Override
                        public Boolean call(Tuple2<Integer, String> integerStringTuple2) throws Exception {


                            return (integerStringTuple2._1()>=sender_bytes);
                        }
                    }).collect();

                    System.setOut(new PrintStream(new FileOutputStream("topSenderBytes")));

                    System.out.println("Below is the list of top Byte Senders");

                    for (Tuple2<?,?> tuple : senders) {

                        System.out.println(tuple._2() + ": " + tuple._1()); //Print IP address and Value(ADU count)
                    }

                    break;
                case 4:

                    System.out.println("\nPlease Note: File \"topReceiverBytes\" will be generated in your current directory with results.");
                    System.out.println("\nPlease Enter the K-Bytes to get Receiver List.");


                    final int receiver_bytes =scan.nextInt();

                    List<Tuple2<Integer,String>> receiver = swapped_receiver_sorted_bytes_rdd.filter(new Function<Tuple2<Integer, String>, Boolean>() {
                        @Override
                        public Boolean call(Tuple2<Integer, String> integerStringTuple2) throws Exception {

                            return (integerStringTuple2._1()>=receiver_bytes);
                        }
                    }).collect();

                    System.setOut(new PrintStream(new FileOutputStream("topReceiverBytes")));


                    System.out.println("Below is the list of top Byte Receivers:");

                    for (Tuple2<?,?> tuple : receiver) {

                        System.out.println(tuple._2() + ": " + tuple._1()); //Print IP address and Value(ADU count)
                    }

                    break;
                case 5:

                    System.out.println("\nPlease Note: File \"ipaddressinfo\" will be generated in your current directory with results.");

                    System.out.println("\nPlease Enter IP Address to get Details (For Example: 10.20.31.64)");


                    String ip_address=scan.next();

                    int valid_flag = 1;
                    String regex = "\\d+";

                    String [] ip_check = ip_address.split("\\.");
                    //Check if all are digits separated with dots.
                    if(ip_check.length==4)
                    {
                        for(int ip_chk_i=0;ip_chk_i<ip_check.length;ip_chk_i++)
                        {
                            if(!(ip_check[ip_chk_i].matches(regex)))
                            {
                                valid_flag=0;
                                break;
                            }

                        }
                    }else
                    {
                        valid_flag=0;
                    }

                    System.setOut(new PrintStream(new FileOutputStream("ipaddressinfo")));


                    if(valid_flag==0)
                    {


                        System.out.println("Incorrect Ip Address Format (Valid values: digits separated by commas)");

                    }else
                    {
                        List<Integer> adu_received = receiver_agg_counts.lookup(ip_address);
                        List<Integer> adu_sent= sender_agg_counts.lookup(ip_address);
                        List<Integer> adu_bytes_rec = receiver_agg_bytes.lookup(ip_address);
                        List<Integer> adu_bytes_sent = sender_agg_bytes.lookup(ip_address);

                        if(adu_bytes_rec.size()==0 && adu_bytes_sent.size()==0 && adu_received.size()==0 && adu_sent.size()==0)
                        {
                            System.out.println("There is no entry for this IP Address");
                        }else {


                            System.out.println("Below are the details for IP Address:" + ip_address);

                            for (int i = 0; i < adu_received.size(); i++) {
                                System.out.println("Number of ADU Received:" + adu_received.get(i));
                            }

                            for (int i = 0; i < adu_sent.size(); i++) {
                                System.out.println("Number of ADU Sent:" + adu_sent.get(i));
                            }

                            for (int i = 0; i < adu_bytes_rec.size(); i++) {
                                System.out.println("Number of ADU Bytes Received:" + adu_bytes_rec.get(i));
                            }

                            for (int i = 0; i < adu_bytes_sent.size(); i++) {
                                System.out.println("Number of ADU Bytes sent:" + adu_bytes_sent.get(i));
                            }
                        }
                    }

                    break;

                default:
                    System.exit(0);
                    break;
            }
        } while(input != 6);

        sc.stop();
    }
}

