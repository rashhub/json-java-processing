package Networking;

import java.io.IOException;
import java.util.*;

/**
 * Created by Rash on 26-05-2017.
 */
public class recommendations {

   public static void main(String args[]) throws IOException
   {
       String [][] itemassociation= new String[3][2];

       itemassociation[0][0]="item1";
       itemassociation[0][1]="item2";

       itemassociation[1][0]="item3";
       itemassociation[1][1]="item4";
       itemassociation[2][0]="item4";
       itemassociation[2][1]="item5";

       String [] results = item(itemassociation);

       for(int i=0;i<results.length;i++)
       {

           System.out.println(results[i]);


       }


       List<Integer> ls = new ArrayList<Integer>();
   }


    public static String[] item(String[][] itemassociation)
    {
        String results[];

        if (itemassociation.length==0)
        {
            return null;
        }else if (itemassociation.length==1)
        {

            results = new String[]{itemassociation[0][0].concat(" ").concat(itemassociation[0][1])};

            return results;
        }

        Map<String,String> map_of_itemassociations = new HashMap<String, String>();


        for(int item_i=0;item_i<itemassociation.length;item_i++) {
            map_of_itemassociations.put(itemassociation[item_i][0], itemassociation[item_i][1]);
        }

        int[] association_size=new int[itemassociation.length];

        int association_size_count=0;
        String value = null;

        for (int item_i=0;item_i<itemassociation.length;item_i++)
        {
           association_size_count=1;

            value= map_of_itemassociations.get(itemassociation[item_i][0]);

            while(value !=null)
            {
                value=map_of_itemassociations.get(value);

                if(value==null)
                {
                    break;
                }

                association_size_count++;

            }

            association_size[item_i]=association_size_count;

        }

        int max =association_size[0];
        int index=0;

        for(int i=0;i<association_size.length;i++)
        {
            if(max<association_size[i])
            {
                max = association_size[i];
                index=i;
            }

        }

        //value=
        String output=itemassociation[index][0];
        value=output;

         while(value !=null)
            {
               value = map_of_itemassociations.get(value);


                if(value==null)
                {
                    break;
                }

                output=output.concat(" ").concat(value);

        }


        results=new String[]{output};


        return results;

    }
}
