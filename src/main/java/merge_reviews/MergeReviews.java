package merge_reviews;

import org.apache.commons.lang.math.NumberUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Rash on 23-02-2017.
 */
public class MergeReviews {

    public static void main(String args[]) throws IOException
    {

        BufferedReader br = new BufferedReader(new FileReader("datafile/reviews.csv"));
        FileWriter ofile =new FileWriter("datafile/merged_reviews.csv");
        BufferedWriter bw = new BufferedWriter(ofile);

        String thisline;
        String old_listing = null;

        String reviews =null;

        List<String> review_data = new ArrayList<String>();


        while((thisline = br.readLine()) != null) {

            review_data.add(thisline);

        }

        bw.write("listing_id,reviews");

        for(int i =1;i<review_data.size();i++)
        {
         String [] data = review_data.get(i).split(",");

            if(!NumberUtils.isNumber(data[0]))
            {
                continue;
            }

            old_listing = data[0];

         String new_listing= data[i+1];

         if(old_listing.equals(new_listing))
         {
             reviews.concat(data[5]);
         }else
         {

             String output = old_listing.concat(",").concat(reviews);

             bw.write(reviews);

             reviews = null;
         }



        }

        bw.close();
        ofile.close();




    }
}
