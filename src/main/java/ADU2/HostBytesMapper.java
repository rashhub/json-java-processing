package ADU2;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class HostBytesMapper
        extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\\s");
        String IPaddr1 = new String();
        String IPaddr2 = new String();
        int last_dot;
        // get the two IP address.port fields
        IPaddr1 = tokens[2];
        IPaddr2 = tokens[4];

        //Check for Valid records
        boolean l_check_valid_rec = false;
        String regex = "\\d+";

        if (tokens.length >=6 ) //check number of tokens
        {
            if(tokens[0].equals("ADU:")) //Ensure String starts with ADU:
            {
               // int countdots = StringUtils.countMatches(IPaddr1,".") +StringUtils.countMatches(IPaddr2,".");

                int countdots = IPaddr1.split("\\.").length -1;

                countdots+= IPaddr2.split("\\.").length-1;

                if(countdots==8) //There should be 8 dots in total for IP Addresses.
                {
                    if(tokens[3].equals(">") || tokens[3].equals("<")) //String contains sender and receiver symbol.
                    {
                        if (tokens[5].matches(regex)) //check if bytes is numeric
                        {
                            l_check_valid_rec = true;
                        }


                    }
                }
            }

        }

        //Emit key and value only when preliminary checks are passed.

        if (l_check_valid_rec == true) {
            // eliminate the port part
            last_dot = IPaddr1.lastIndexOf('.');
            IPaddr1 = IPaddr1.substring(0, last_dot);
            last_dot = IPaddr2.lastIndexOf('.');
            IPaddr2 = IPaddr2.substring(0, last_dot);

            //Define variables to hold values for sender and receiver
            String SentBytes = null;
            String ReceivedBytes = null;
            String SenderKey = null;
            String ReceiveKey = null;

            // Check for sender ip and receiver ip. Emit two rows as below:
            // IP Adress 1 senderbytes receivedbytes -- sender will have bytes in sentbytes and 0 received bytes
            // IP Address 2 senderbytes receivedbytes -- sent bytes will be 0, receivedbytes will be number of bytes received.

            if (tokens[3].equals(">")) {
                SenderKey = IPaddr1;
                ReceiveKey = IPaddr2;

            } else {
                SenderKey = IPaddr2;
                ReceiveKey = IPaddr1;

            }

            SentBytes = tokens[5].concat(" ").concat("0"); //received column is 0
            ReceivedBytes = "0".concat(" ").concat(tokens[5]); //send column is 0

            //emit one for sender and one for receiver. sender will have 0 bytes received, receiver will have 0 bytes sent.
            context.write(new Text(SenderKey), new Text(SentBytes));
            context.write(new Text(ReceiveKey), new Text(ReceivedBytes));
        }
    }
}

