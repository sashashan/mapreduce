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
import java.lang.Long;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

/** 
 * Phase1 of Hadoop Block Nested Loop KNN Join (H-BNLJ).
 */
public class NPhase1 extends Configured implements Tool 
{
	public static class MapClass extends MapReduceBase
	implements Mapper<LongWritable, Text, IntWritable, NPhase1Value> 
	{
		private int numberOfPartition;
		private int dimension;
		private int scale = 1000;
		private int fileId = 0;
		private String inputFile;
		private String mapTaskId;
		private Random r;
		private int recIdOffset;
		private int coordOffset;

		public void configure(JobConf job) 
		{
			inputFile = job.get("map.input.file");
			mapTaskId = job.get("mapred.task.id");
			numberOfPartition = job.getInt("numberOfPartition", 2);
			dimension = job.getInt("dimension", 2);

			recIdOffset = 0;
			coordOffset = recIdOffset + 1;

			r = new Random();

			if (inputFile.indexOf("outer") != -1)  
				fileId = 0;
			else if (inputFile.indexOf("inner") != -1)
				fileId = 1;
			else 
			{
				System.out.println("Invalid input file source@NPhase1");
				System.exit(-1);
			}
		} // configure
		
		/**
		 * Partition the input data sets (R and S) into multiple buckets. 
		 */  
		public void map(LongWritable key, Text value, 
		OutputCollector<IntWritable, NPhase1Value> output, 
		Reporter reporter) throws IOException 
		{
			String[] parts = value.toString().split(" +");	
			String recId = parts[recIdOffset];
			int recIdInt = Integer.parseInt(recId);
			float[] coord = new float[dimension];
			for (int i = 0; i < dimension; i++) 
				coord[i] = Float.parseFloat(parts[coordOffset + i]);	

			// Required if we want to compare the results with H-zKNNJ
			int[] converted = new int[dimension];
			float[] tmp_coord = new float[dimension];
			for (int i = 0; i < dimension; i++) {
				tmp_coord[i] = coord[i];
				converted[i] = (int) tmp_coord[i];
				tmp_coord[i] -= converted[i];
				converted[i] *=scale;
				//converted[i] += (int) (tmp_coord[i] * scale);
				converted[i] += (tmp_coord[i] * scale);
			}
	
			// Use scaled data sets
			for (int i = 0; i < dimension; i++) 
				coord[i] = (float) converted[i];	

			NPhase1Value np1v = new NPhase1Value(recIdInt, coord, (byte) fileId, dimension);

			//Random generate a partition ID for an input record
			int partID = r.nextInt(numberOfPartition);
			int groupID = 0;

			for (int i = 0; i < numberOfPartition; i++) 
			{
				if (fileId == 0)
					groupID = partID * numberOfPartition + i;
				else if (fileId == 1)
					groupID = partID + i * numberOfPartition;
				else 
				{
					System.out.println("The record comes from unknow file!!!");	
					System.exit(-1);
				}

				IntWritable	mapKey = new IntWritable(groupID);
				output.collect(mapKey, np1v);
			} 
		} // map
	} // MapClass
  
	/** 
	 * Perform Block Nested Loop join for records in the same partition/bucket.
	 */
	public static class Reduce extends MapReduceBase
	implements Reducer<IntWritable, NPhase1Value, NullWritable, Text> 
	{
		private int bufferSize = 8 * 1024 * 1024; 
		private MultipleOutputs mos;

		private LocalDirAllocator lDirAlloc =
			new LocalDirAllocator("mapred.local.dir");
		private FSDataOutputStream out;
		private FileSystem localFs;	
		private FileSystem lfs;	
		private Path file1;
		private Path file2;

		private int numberOfPartition;
		private int dimension;
		private int blockSize;
		private int knn;
		//private boolean self_join;

		private Configuration jobinfo;
		
		public void configure(JobConf job) 
		{
			numberOfPartition = job.getInt("numberOfPatition", 2);
			dimension = job.getInt("dimension", 2);
			blockSize = job.getInt("blockSize", 1024);
			knn = job.getInt("knn", 1024);
			//self_join = Boolean.valueOf(job.get("self_join"));
			
			// Get the local file system
			try 
			{
				localFs = FileSystem.getLocal(job);
			} catch (IOException e) 
			{
				e.printStackTrace();
			}
	
			lfs = ((LocalFileSystem) localFs).getRaw();
			jobinfo = job;
			//mos = new MultipleOutputs(job);
		}
		
		public void reduce(IntWritable key, Iterator<NPhase1Value> values,
		OutputCollector<NullWritable, Text> output, 
		Reporter reporter) throws IOException 
		{
			String algorithm = "nested_loop";
			String prefix_dir = algorithm + "-" + Integer.toString(numberOfPartition) + "-" + key.toString();

			try {
				file1 = lDirAlloc.getLocalPathForWrite(prefix_dir + "/" + "outer", jobinfo); 
				file2 = lDirAlloc.getLocalPathForWrite(prefix_dir + "/" + "inner", jobinfo);
				lfs.create(file1);
				lfs.create(file2);
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}

			String outerTable = file1.toString();
			String innerTable = file2.toString();
			FileWriter fwForR	= new FileWriter(outerTable);
			FileWriter fwForS	= new FileWriter(innerTable);
			BufferedWriter bwForR = new BufferedWriter(fwForR, bufferSize);
			BufferedWriter bwForS = new BufferedWriter(fwForS, bufferSize);
	
			while (values.hasNext()) 
			{
				// Value format <rid, coord, src>
				NPhase1Value np1v = values.next();
				String record = np1v.getFirst().toString();
				String[] parts = np1v.getSecond().toStrings();
				int srcId = (int)np1v.getThird().get();

				for (int i = 0; i < dimension; i++)
					record = record + " " + parts[i];

				if (srcId == 0) {
					bwForR.write(record + "\n");
				} else if (srcId == 1) {
					bwForS.write(record + "\n");
				} else {
					System.out.println("unknown file number");
					System.exit(-1);
				}
			} 

			reporter.progress();
			bwForR.close();
			bwForS.close();
			fwForR.close();	
			fwForS.close();

			FileReader frForR = new FileReader(outerTable);
			BufferedReader brForR = new BufferedReader(frForR, bufferSize);

			// initialize for R
			int number = blockSize;
			int[] idR = new int[number];
			float[][] coordR = new float[number][dimension];
			ArrayList<PriorityQueue> knnQueueR = new ArrayList<PriorityQueue>(number);

			// Create priority queue with specified comparator
			Comparator<ListElem> rc = new RecordComparator();
			for (int j = 0; j < number; j++) 
			{
				PriorityQueue<ListElem> knnQueue = new PriorityQueue<ListElem>(knn + 1, rc);
				knnQueueR.add(knnQueue);
			}

			boolean flag = true;
			while (flag) 
			{
				// Read a block of R
				for (int ii = 0; ii < number; ii++) 
				{
					String line = brForR.readLine();
					if (line == null) 
					{
						flag = false;     //Going to end
						number = ii;
						break;
					}

					String parts[] = line.split(" +");
					int id1 = Integer.valueOf(parts[0]);
					idR[ii] = id1;

					int st = 1;
					float[] x = coordR[ii];	
					for (int i = 0; i < dimension; i++) 
						x[i] = Float.valueOf(parts[st + i]);
				}

				//if (self_join) innerTable = outerTable;
				// For all records in a block of R, the following carries out knn-join with S
				FileReader frForS = new FileReader(innerTable);
				BufferedReader brForS = new BufferedReader(frForS, bufferSize);

				while(true) 
				{
					String line = brForS.readLine();
					if (line == null) break;
					String parts[] = line.split(" +");
					int id2 = Integer.valueOf(parts[0]);

					int st = 1;
					float[] y = new float[dimension];
					for (int i = 0; i < dimension; i++)
						y[i] = Float.valueOf(parts[st + i]);

					float[] distArray = new float[number];
					for (int i = 0; i < number; i++) 
					{
						distArray[i] = 0;
						float[] x = coordR[i];
						for (int k = 0; k < dimension; k++) 
							distArray[i] += (x[k] - y[k]) * (x[k] - y[k]);
						
						ListElem ne = new ListElem(dimension, distArray[i], id2);
						PriorityQueue<ListElem> knnQueue = knnQueueR.get(i);
						knnQueue.add(ne);
						if (knnQueue.size() > knn) 
							knnQueue.poll();
					} // [0 . . number - 1]
				} // while - inner

				brForS.close(); 
				frForS.close();	

				for (int j = 0; j < number; j ++) 
				{
					PriorityQueue<ListElem> knnQueue = knnQueueR.get(j);
					int id1 = idR[j];
					for (int i = 0; i < knn; i++) 
					{
						ListElem e = knnQueue.poll();
						output.collect(
							NullWritable.get(), 
							new Text( id1 + " " + Integer.toString(e.getId()) + " " + Float.toString(e.getDist()) )
						);
					} // for
				}
				reporter.progress();
			} // while - outer
			brForR.close();
			frForR.close();
		} // reduce

        public void close() throws IOException 
		{
			//mos.close();
		}
						        
	} // Reducer
  
	static int printUsage() 
	{
		System.out.println(
			"NPhase1 [-m <maps>] [-r <reduces>] [-p <numberOfPartitions>] " 
			+ "[-d <dimension>] [-k <knn>] [-b <blockSize(#records) for R>] " 
			+ "<input (R)> <input (S)> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
  
	/**
	 * The main driver for H-BNLJ program.
	 * Invoke this method to submit the map/reduce job.
	 * @throws IOException When there is communication problems with the 
	 * job tracker.
	 */
	public int run(String[] args) throws Exception 
	{
		int numberOfPartition = 2;
		//boolean self_join = false;
		JobConf conf = new JobConf(getConf(), NPhase1.class);
		conf.setJobName("NPhase1");

		conf.setMapperClass(MapClass.class);        
		conf.setReducerClass(Reduce.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(NPhase1Value.class);
		
		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) 
		{
			try {
				if ("-m".equals(args[i])) {
					//conf.setNumMapTasks(Integer.parseInt(args[++i]));
					++i;
				} else if ("-r".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else if ("-p".equals(args[i])) {
					numberOfPartition = Integer.parseInt(args[++i]);
					conf.setInt("numberOfPartition", numberOfPartition);
				} else if ("-d".equals(args[i])) {
					conf.setInt("dimension", Integer.parseInt(args[++i]));
				} else if ("-k".equals(args[i])) {
					conf.setInt("knn", Integer.parseInt(args[++i]));
				} else if ("-b".equals(args[i])) {
					conf.setInt("blockSize", Integer.parseInt(args[++i]));
/*				} else if ("-sj".equals(args[i])) {
					self_join = Boolean.parseBoolean(args[++i]);
					conf.set("self_join", Boolean.toString(self_join));
*/				} else {
					other_args.add(args[i]);
				}
				// set the number of reducers
				conf.setNumReduceTasks(numberOfPartition * numberOfPartition);
			} 
			catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " + args[i-1]);
				return printUsage();
		  	}
		}

		if (other_args.size() != 3) {
			System.out.println("ERROR: Wrong number of parameters: " + other_args.size() + " instead of 3.");
		  return printUsage();
		}

		FileInputFormat.setInputPaths(conf, other_args.get(0));
		//System.out.println("set R to  the input path");
		FileInputFormat.addInputPaths(conf, other_args.get(1));
		//System.out.println("set S to  the input path");
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(2)));

		JobClient.runJob(conf);

		return 0;
	} // run
  
	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new NPhase1(), args);
		System.exit(res);
	}
} //NPhase1
