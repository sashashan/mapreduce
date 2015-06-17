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
import org.apache.hadoop.mapred.lib.MultipleOutputs;

/**
 * Phase3 of Hadoop zvalue KNN Join (H-zKNNJ).
 */
public class BPhase3 extends Configured implements Tool {
	public static class MapClass extends MapReduceBase 
		implements Mapper<LongWritable, Text, IntWritable, BPhase3Value> {

		public void map(LongWritable key, Text value, 
				OutputCollector<IntWritable, BPhase3Value> output, 
				Reporter reporter) 
			throws IOException {

			String line = value.toString();
			String[] parts = line.split(" +");

			// key format <rid1>
			IntWritable mapKey = 
				new IntWritable(Integer.valueOf(parts[0].trim()));

			// value format <rid2, dist>
			BPhase3Value np2v = new BPhase3Value(Integer.valueOf(parts[1]),
					Float.valueOf(parts[2]));

			output.collect(mapKey, np2v);
		}
	}
  
	public static class Reduce extends MapReduceBase
		implements Reducer<IntWritable, BPhase3Value, NullWritable, Text> {
		int knn;
		
		class Record {
			public int id2;
			public float dist;

			Record(int id2, float dist) {
				this.id2 = id2;
				this.dist = dist;
			}

			public String toString() {
				return Integer.toString(id2) + " " + Float.toString(dist);	
			} 
		}

		class RecordComparator implements Comparator<Record> {
			public int compare(Record o1, Record o2) {
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

		public void configure(JobConf job) {
			knn = job.getInt("knn", 3);
		}	

		public void reduce(IntWritable key, Iterator<BPhase3Value> values, 
			OutputCollector<NullWritable, Text> output, 
			Reporter reporter) throws IOException {

			RecordComparator rc = new RecordComparator();
			PriorityQueue<Record> pq = new PriorityQueue<Record>(knn + 1, rc);

			TreeSet<Integer> ts = new TreeSet<Integer>();

			// For each record we have a reduce task
			// value format <rid1, rid2, dist>
			while (values.hasNext()) {
				BPhase3Value np2v = values.next();

				int id2 = np2v.getFirst().get();

               if (!ts.contains(id2)) {
                   ts.add(id2);
		       	float dist = np2v.getSecond().get();
		       	Record record = new Record(id2, dist);
		       	pq.add(record);
		       	if (pq.size() > knn)
		       		pq.poll();
               }
			}

			while(pq.size() > 0) {
				output.collect(NullWritable.get(),
				   new Text(key.toString() + " " + pq.poll().toString()));
				//break; // we only keep the first one
			}

		} // reduce
	} // Reducer
 
	static int printUsage() {
		System.out.println(
			"NPhase1 [-m <maps>] [-r <reduces>] [-k <knn>] " 
			+ "<input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
  
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), BPhase3.class);
		conf.setJobName("BPhase3");

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(BPhase3Value.class);
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class);		

		conf.setMapperClass(MapClass.class);        
		conf.setReducerClass(Reduce.class);

		List<String> other_args = new ArrayList<String>();
		for(int i = 0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					//conf.setNumMapTasks(Integer.parseInt(args[++i]));
					++i;
				} else if ("-r".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else if ("-k".equals(args[i])) {
					int knn = Integer.parseInt(args[++i]);
					conf.setInt("knn", knn);
					//System.out.println(knn);
				} else {
					other_args.add(args[i]);
				}
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
		return 0;
	}
  
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BPhase3(), args);
	}

}


