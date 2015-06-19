/*
 * THERE IS NO WARRANTY FOR THE PROGRAM, TO THE EXTENT PERMITTED BY APPLICABLE LAW.
 * EXCEPT WHEN OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER
 * PARTIES PROVIDE THE PROGRAM "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER
 * EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE ENTIRE RISK AS
 * TO THE QUALITY AND PERFORMANCE OF THE PROGRAM IS WITH YOU. SHOULD THE PROGRAM
 * PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL NECESSARY SERVICING, REPAIR
 * OR CORRECTION.
 */
package test;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.lang.management.*;

/**
 * Phase2 of Hadoop Block R*-tree KNN Join (H-BRJ).
 */
public class RPhase2 extends Configured implements Tool 
{
	public static class MapClass extends MapReduceBase 
	implements Mapper<LongWritable, Text, IntWritable, RPhase2Value> 
	{
		public static int count = 0;
		public void configure(JobConf job) 
		{	
			System.out.println("############### Configuring a mapper! File number: " + count);
			System.out.println("###############: PID" + ManagementFactory.getRuntimeMXBean().getName());
			count ++;
		}

		public void map(LongWritable key, Text value, 
		OutputCollector<IntWritable, RPhase2Value> output, 
		Reporter reporter) 	throws IOException
		{
			String line = value.toString();
			String[] parts = line.split(" +");
			// key format <rid1>
			IntWritable mapKey = new IntWritable(Integer.valueOf(parts[0]));
			// value format <rid2, dist>
			RPhase2Value np2v = new RPhase2Value(Integer.valueOf(parts[1]), Float.valueOf(parts[2]));

			System.out.println("############### key:  " + mapKey.toString() + "   np2v:  " + np2v.toString());
			output.collect(mapKey, np2v);
		}
	}
  
	public static class Reduce extends MapReduceBase
	implements Reducer<IntWritable, RPhase2Value, NullWritable, Text> 
	{
		int numberOfPartition;	
		int knn;
		
		class Record 
		{
			public int id2;
			public float dist;


			Record(int id2, float dist) 
			{
				this.id2 = id2;
				this.dist = dist;

				System.out.println("########### Creatng a record with id2: " + id2 + "and dist: " + dist);
			}

			public String toString() 
			{
				return Integer.toString(id2) + " " + Float.toString(dist);	
			} 
		}

		class RecordComparator implements Comparator<Record> 
		{
			public int compare(Record o1, Record o2) 
			{
				int ret = 0;
				float dist = o1.dist - o2.dist;

				if (Math.abs(dist) < 1E-6)
					ret = o1.id2 - o2.id2;
				else if (dist > 0)
					ret = 1;
				else 
					ret = -1;

				return -ret;
			}	
		}

		public void configure(JobConf job) 
		{
			numberOfPartition = job.getInt("numberOfPartition", 3);	
			knn = job.getInt("knn", 3);
			System.out.println("########## configuring!");
		}	

		public void reduce(IntWritable key, Iterator<RPhase2Value> values, 
		OutputCollector<NullWritable, Text> output, 
		Reporter reporter) throws IOException 
		{
			//initialize the pq
			RecordComparator rc = new RecordComparator();
			PriorityQueue<Record> pq = new PriorityQueue<Record>(knn + 1, rc);

			System.out.println("Phase 2 is at reduce");
			System.out.println("########## key: " + key.toString());

			// For each record we have a reduce task
			// value format <rid1, rid2, dist>
			while (values.hasNext()) 
			{
				RPhase2Value np2v = values.next();

				int id2 = np2v.getFirst().get();
				float dist = np2v.getSecond().get();
				Record record = new Record(id2, dist);
				pq.add(record);
				if (pq.size() > knn)
					pq.poll();
			}

			while(pq.size() > 0) 
			{
				output.collect(NullWritable.get(), new Text(key.toString() + " " + pq.poll().toString()));
				//break; // only ouput the first record
			}

		} // reduce
	} // Reducer
 
	static int printUsage() 
	{
		System.out.println(
			"NPhase1 [-m <maps>] [-r <reduces>] [-p <numberOfPartitions>] " 
			+ "[-k <knn>] " + "<input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
  
	/**
	 * The main driver for H-BNLJ program.
	 * Invoke this method to submit the map/reduce job.
	 * @throws IOException When there is communication problems with the 
	 *                     job tracker.
	 */
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), RPhase2.class);
		conf.setJobName("RPhase2");

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(RPhase2Value.class);

		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class);	

		conf.setMapperClass(MapClass.class);        
		conf.setReducerClass(Reduce.class);
	
		int numberOfPartition = 0; // IMPORTANT TO NOT SET IT TO ZERO HERE!! - Sasha
		List<String> other_args = new ArrayList<String>();
		System.out.println("Greetings from Sasha!");

		for(int i = 0; i < args.length; ++i) 
		{
			try {
				if ("-m".equals(args[i])) {
					//conf.setNumMapTasks(Integer.parseInt(args[++i]));
					++i;
				} else if ("-p".equals(args[i])) {
					numberOfPartition = Integer.parseInt(args[++i]);
					conf.setInt("numberOfPartition", numberOfPartition);
				} else if ("-k".equals(args[i])) {
					int knn = Integer.parseInt(args[++i]);
					conf.setInt("knn", knn);
					System.out.println(knn + "~ hi");
				} else {
					other_args.add(args[i]);
 	 			}
				conf.setNumReduceTasks(numberOfPartition * numberOfPartition); // why is this param changed to p^2? -Sasha
				//conf.setNumReduceTasks(1);
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " + args[i-1]);
				return printUsage();
			}
		} 

		// Make sure there are exactly 2 parameters left.
		if (other_args.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: " +
				other_args.size() + " instead of 2.");
			return printUsage();
		}

		FileInputFormat.setInputPaths(conf, other_args.get(0));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		JobClient.runJob(conf);
		/*
		JobClient is the primary interface for the user-job to interact with the cluster. 
		JobClient provides facilities to submit jobs, track their progress, access component-tasks' 
		reports/logs, get the Map-Reduce cluster status information etc.
		-Sasha
		*/
		return 0;
	}
  
	public static void main(String[] args) throws Exception {
		System.out.println("Hi hi!");
		int res = ToolRunner.run(new Configuration(), new RPhase2(), args);
	}
} // RPhase2


