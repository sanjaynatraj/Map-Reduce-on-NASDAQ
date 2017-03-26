package sanjay;

import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;




public class Main {

	public static void main(String[] args) throws Exception {
		long start = new Date().getTime();
		Configuration config = new Configuration();
		
		Job job1 = Job.getInstance();
		job1.setJarByClass(StockVolatility.class);
		Job job2 = Job.getInstance();
		job2.setJarByClass(StockVolatility_2.class);
		Job job3 = Job.getInstance();
	    job3.setJarByClass(StockVolatility_3.class);
	   
	    System.out.println("\n**********Stock Volatility**********\n");
		
		job1.setJarByClass(StockVolatility.class);
		job1.setMapperClass(StockVolatility.Map1.class);
		job1.setReducerClass(StockVolatility.Red1.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		int NOfReducer1 = Integer.valueOf(2);	
		job1.setNumReduceTasks(NOfReducer1);
		
		job2.setJarByClass(StockVolatility_2.class);
		job2.setMapperClass(StockVolatility_2.Map2.class);
		job2.setReducerClass(StockVolatility_2.Red2.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		int NOfReducer2 = Integer.valueOf(2);	
		job2.setNumReduceTasks(NOfReducer2);
		
		
		job3.setJarByClass(StockVolatility_3.class);
		job3.setMapperClass(StockVolatility_3.Map3.class);
		job3.setReducerClass(StockVolatility_3.Red3.class);
		
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
        int NOfReducer3 = 1;	
		job3.setNumReduceTasks(NOfReducer3);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));		
		FileOutputFormat.setOutputPath(job1, new Path("Sanjay_Inter_"));
		
		FileInputFormat.addInputPath(job2, new Path("Sanjay_Inter_"));
		FileOutputFormat.setOutputPath(job2, new Path("Sanjay_Output_"));
		
		FileInputFormat.addInputPath(job3, new Path("Sanjay_Output_"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		

		job1.waitForCompletion(true);
		job2.waitForCompletion(true);
		//job3.waitForCompletion(true);
//		boolean status = job.waitForCompletion(true);
		boolean status3 = job3.waitForCompletion(true);
		if (status3 == true) {
			long end = new Date().getTime();

			System.out.println("\nJob took " + (end-start)/1000 + "seconds\n");
		}
		System.out.println("\n**********Stock Volatility**********\n");		

	}
					
				
					
	}


