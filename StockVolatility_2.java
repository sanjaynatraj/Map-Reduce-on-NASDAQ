package sanjay;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class StockVolatility_2 {
	 public static class Map2 extends Mapper< Object,Text,Text,Text >  {
			private Text key2 = new Text();
			private Text value2 = new Text();
	        public void map(Object key, Text values, Context context)throws IOException, InterruptedException {
				String line1 = values.toString(); // value to string
				System.out.println("Line is "+line1);
				String[] cols = line1.split("\\|"); // initalize the column array
				System.out.println("Reached here");
             key2.set(cols[0]);                // Set the key as filename
             System.out.println("Key is "+cols[0]);
             value2.set(cols[cols.length-1]);                // Set the value as xi
             System.out.println("Written value is "+cols[cols.length-1]);
             context.write(key2, value2);
	       }
	 }
		
	  
	    public static class Red2 extends Reducer<Text, Text, Text, Text> {
		    private Text o_key2 = new Text();
			private Text o_value2 = new Text();
	        private ArrayList<String> alist = new ArrayList<String>();   
			public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
					System.out.println("key is "+key.toString());
					String line1 = values.toString();
					String keys1 = key.toString();
					//Double volatility = Double.parseDouble(line1);
		            double sum=0;
					for (Text text : values) {
						System.out.println("Adding the "+text);
						alist.add(text.toString());
						sum++;
					}   
	                        
					double totalsum=0;
					for (int i = 0; i < alist.size(); i++) {
						 totalsum = totalsum + Double.parseDouble(alist.get(i).toString());
					
					}
                  double xbar = totalsum/sum,divide=0;
                  for (String list1 : alist) {
						double subtract = Double.parseDouble(list1)-xbar;
						double square = Math.pow(subtract,2);
						divide = square/(sum-1);
					}  				   		            
                  double volatility= Math.sqrt(divide);   
                  o_key2.set(keys1+"|");
                  o_value2.set("|"+Double.toString(volatility));
                  context.write(o_key2,o_value2);
				}
	    }
}
