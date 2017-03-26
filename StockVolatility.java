package sanjay;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import sun.text.resources.cldr.ee.FormatData_ee;

	public class StockVolatility {

			public static class Map1 extends Mapper<Object, Text, Text, Text> {
				private Text key1 = new Text();
                private Text value1 = new Text();
				public void map(Object key, Text values, Context context)throws IOException, InterruptedException {

					String line = values.toString();                       // value to string
					FileSplit fileSplit = (FileSplit)context.getInputSplit();
					String filename = fileSplit.getPath().getName();       //split the file and get the context(name of the company)
					if(!line.contains("Date"))
					{	
					String[] column = line.split(",");                     // initialize the column array
					String[] date = column[0].split("-");                  //split the date into month date year
					key1.set(filename + "|" + date[0] + "|" + date[2]);    // set the key to cmpname, month, year								                       // company and its date
				    value1.set(date[1] + "|" + column[6]);              // set the value to the  and the adjusted column price
	                context.write(key1, value1);                        // write key and value
	                }
				}
	 
			}	

		
          public static class Red1 extends Reducer<Text, Text, Text, Text> {
			private Text o_key = new Text();                              // reducer is initialized to compute xi 
			private Text o_value = new Text();
            private ArrayList<String> date = new ArrayList<String>();
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
				String value = values.toString();
                String keys = key.toString();
                HashMap<String,String> table  = new HashMap<String,String>();
                
                for (Text val : values) {
			    	
                	System.out.println("Value is "+val.toString());
			    	String[] splitval = val.toString().split("\\|");
			    	System.out.println("Split val is "+splitval[0].toString()+" "+splitval[1].toString());
			    	table.put(splitval[0].toString(), splitval[1].toString());
			        date.add(splitval[0].toString());    
		  } 
                Collections.sort(date);                        // Sorting the date in arraylist
			    String firstelement = date.get(0);             // To get first element
			    String lastelement = date.get(date.size()-1);  // To return the last element
			    String price1 = table.get(firstelement);
			    String price2 = table.get(lastelement);
			    
			    if(price1!=null && price2!=null)
			    {
			    System.out.println("Price is "+price1);
			    System.out.println("Price 2 is "+price2);
				double end_price = Double.parseDouble(price1); // To find the ending adjusted close price				
				double first_price =Double.parseDouble(price2); // To find the ending adjusted close price
				double xi = (end_price-first_price)/(first_price); // Calculate Xi
				String rateofreturn = Double.toString(xi);
				o_key.set(keys);
				o_value.set("|"+rateofreturn);
				context.write(o_key, o_value);
			    }
			}			
          }
	}
	  
      


		
	
