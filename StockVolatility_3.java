package sanjay;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class StockVolatility_3 {

    public static class Map3 extends Mapper< Object,Text,Text,Text >  {
		private Text key3 = new Text();
		private Text value3 = new Text();
        public void map(Object key, Text values, Context context)throws IOException, InterruptedException {
			String line2 = values.toString(); 
			String keys2 = key.toString();
      		key3.set("key");		                               
            value3.set(line2);
			context.write(key3, value3);
       }
    }
        
        public static class Red3 extends Reducer<Text, Text, Text, Text> {
		    private Text o_key3 = new Text();
			private Text o_value3 = new Text();
	        private ArrayList<String> blist = new ArrayList<String>();
	        private ArrayList<String> clist = new ArrayList<String>();
			public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			
		
			HashMap<String,String> table  = new HashMap<String,String>();
            TreeMap<String,String> tree = new TreeMap<String,String>();                
       
            for (Text val : values) {
                	String[] cols = val.toString().split("//|");
			    	table.put( cols[2],cols[0]);
			    	System.out.println("Adding the value "+cols[0].toString());
			    	System.out.println("Key being added is "+cols[2].toString());
			         
                }
            tree.putAll(table); 
            
            Iterator<String> it = tree.keySet().iterator();
            Object obj;
            int sum = 0;
            while (it.hasNext()&& sum<10) {
              obj = it.next();
              System.out.println(obj + ": " + tree.get(obj));
              sum++;
              //String test=it.next();
              o_key3.set(blist.get(Integer.parseInt(it.next())));
 			  o_value3.set(it.next().toString());
 			  context.write(o_key3, o_value3);
            }
            
           
               Iterator<String> iterator = tree.descendingKeySet().iterator();
               sum = 0;
               while (it.hasNext()&& sum<10) {
                   obj = it.next();
                   System.out.println(obj + ": " + tree.get(obj));
                   sum++;
                   o_key3.set(clist.get(Integer.parseInt(it.next())));
      			   o_value3.set(it.next().toString());
      			   context.write(o_key3, o_value3);
               }           
               
               
			}    
}
}
