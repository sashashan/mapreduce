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

import de.lmu.ifi.dbs.elki.data.FloatVector;
import de.lmu.ifi.dbs.elki.data.DoubleVector;
import de.lmu.ifi.dbs.elki.database.DistanceResultPair;
import de.lmu.ifi.dbs.elki.distance.FloatDistance;
import de.lmu.ifi.dbs.elki.distance.DoubleDistance;
import de.lmu.ifi.dbs.elki.distance.distancefunction.DistanceFunction;
import de.lmu.ifi.dbs.elki.distance.distancefunction.EuclideanDistanceFunction;
import de.lmu.ifi.dbs.elki.index.tree.spatial.rstarvariants.rstar.RStarTree;
import de.lmu.ifi.dbs.elki.index.tree.spatial.*;
import de.lmu.ifi.dbs.elki.index.tree.TreeIndex;
import de.lmu.ifi.dbs.elki.index.*;
import de.lmu.ifi.dbs.elki.parser.*;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.ParameterException;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameterization.ListParameterization;

/**
 * Phase1 of Hadoop Block R*-tree KNN Join (H-BRJ).
 */ 

public class RPhase1 extends Configured implements Tool 
{
	public static final int MB = 1024 * 1024;
	public static final int KB = 1024;
	public static final int scale = 1000;

	public static class MapClass extends MapReduceBase
	implements Mapper<LongWritable, Text, RPhase1Key, RPhase1Value> 
	{
		private int numberOfPartition;
		private int dimension;
		private int fileId = 0;
		private String inputFile;
		private Random r;
		private int recIdOffset;
		private int coordOffset;

		public void configure(JobConf job) 
		{
			inputFile = job.get("map.input.file");
			numberOfPartition = job.getInt("numberOfPartition", 2);
			dimension = job.getInt("dimension", 2);

			recIdOffset = 0;
			coordOffset = recIdOffset + 1;

			r = new Random();

			if (inputFile.indexOf("outer") != -1)  
				fileId = 0;
			else if (inputFile.indexOf("inner") != -1)
				fileId = 1;
			else {
				System.out.println("Invalid input file source@NPhase1");
				System.exit(-1);
			}
		} //configure
   
		/**
		 * Partition input data sets into multiple buckets
		 */ 
		public void map(LongWritable key, Text value, 
		OutputCollector<RPhase1Key, RPhase1Value> output, 
		Reporter reporter) throws IOException 
		{
			String[] parts = value.toString().split(" +");	
			String recId = parts[recIdOffset];
			int recIdInt = Integer.parseInt(recId);
			float[] coord = new float[dimension];
			for (int i = 0; i < dimension; i++) {
				coord[i] = Float.parseFloat(parts[coordOffset + i]);	
			}

			int[] converted_coord = new int[dimension];
			for (int i = 0; i < dimension; i++)	
			{
				converted_coord[i] = (int) coord[i];        // get the integer part
				coord[i] = coord[i] - converted_coord[i];   // get the fraction part
				converted_coord[i] *= scale;                // scale integer part
				converted_coord[i] += coord[i] * scale;     // scale fraction part
			}

			String zval = Zorder.valueOf(dimension, converted_coord);

			// Map output value format <zval, recID, src>
			// The coordinates of a record will be converted from zval 
			// in rstar tree bulkload method. Therefore no need to transfer
			// coordinates in this stage, and as a result, we have better
			// performace. 
			RPhase1Value rp1v = new RPhase1Value(zval, recIdInt, (byte) fileId);

			//Random generate a partition ID for an input record
			int partID = r.nextInt(numberOfPartition);
			int groupID = 0;

			for (int i = 0; i < numberOfPartition; i++) {
				if (fileId == 0) {
					groupID = partID * numberOfPartition + i;
				} else if (fileId == 1) {
					groupID = partID + i * numberOfPartition;
				} else {
					System.out.println("The record comes from unknow file!!!");	
					System.exit(-1);
				}

				RPhase1Key rp1k = new RPhase1Key(zval, recIdInt, groupID);		

				//value format  <rid, coord, src>
				output.collect(rp1k, rp1v);
			} 
		} // map
	} //mapper
  
	/**
	 * Perform R*-tree based KNN Join for each partition/bucket.
	 */
	public static class Reduce extends MapReduceBase
	implements Reducer<RPhase1Key, RPhase1Value, NullWritable, Text> 
	{
		private int bufferSize = 8 * MB; 
		private LocalDirAllocator lDirAlloc = new LocalDirAllocator("mapred.local.dir");
		private FileSystem localFs;	
		private FileSystem lfs;	
		private Path file1;
		private Path file2;

		private int numberOfPartition;
		private int dimension;
		private int knn;
		private int blockSize;

		private Configuration jobinfo;
		
		public void configure(JobConf job) 
		{
			numberOfPartition = job.getInt("numberOfPartition", 2);
			dimension = job.getInt("dimension", 2);
			knn = job.getInt("knn", 3);
			bufferSize = job.getInt("block", 8) * MB;
			
			try {
				localFs = FileSystem.getLocal(job);
			} catch (IOException e) {
				e.printStackTrace();
			}
	
			lfs = ((LocalFileSystem) localFs).getRaw();
			jobinfo = job;
			//mos = new MultipleOutputs(job);
		}
		
		public void reduce(RPhase1Key key, Iterator<RPhase1Value> values,
		OutputCollector<NullWritable, Text> output, 
		Reporter reporter) throws IOException 
		{
			Text reduceKey = null;
			Text reduceValue = null;

			String recId = null;
			String recValueX = null; 
			String recValueY = null; 
			String recDesc = null;
			int fileId  = 0;
			 
			int sum = 0;

			long rid, rvalue;
			String algorithm = "hbrj";

			String prefix_dir = algorithm + "-" + Integer.toString(numberOfPartition) + "-" +
				key.getThird().toString() + "-" + knn;

			// Create seperate local files for different key value
			try {
				file1 = lDirAlloc.getLocalPathForWrite(prefix_dir + "/" + "outer", jobinfo); 
				file2 = lDirAlloc.getLocalPathForWrite(prefix_dir + "/" + "inner", jobinfo);
				lfs.create(file1);
				lfs.create(file2);
			} catch (IOException e) {
				e.printStackTrace();
			}

			// Save data to local file
			String outerTable = file1.toString();
			String innerTable = file2.toString();
			FileWriter fwForR	= new FileWriter(outerTable);
			FileWriter fwForS	= new FileWriter(innerTable);
			BufferedWriter bwForR = new BufferedWriter(fwForR, bufferSize);
			BufferedWriter bwForS = new BufferedWriter(fwForS, bufferSize);

			int innerSize = 0;

			while (values.hasNext()) 
			{
				// Value format <zval, rid, src>
				RPhase1Value rp1v = values.next();
				String record = rp1v.getFirst().toString();   //zval
				record += " " + rp1v.getSecond().toString();  //rid
				byte srcId = rp1v.getThird().get();

				if (srcId == 0) {
					bwForR.write(record + "\n");
				} else if (srcId == 1) {
					bwForS.write(record + "\n");
					innerSize++;
				} else {
					System.out.println("unknow file number");
					System.exit(-1);
				}
			} 

			reporter.progress();
			bwForR.close();
			bwForS.close();
			fwForR.close();	
			fwForS.close();

			int blockSize = 128 * KB;
			int cacheSize = 64 * MB;
			String indexFile = new String(innerTable + ".index");
			ListParameterization spatparams = new ListParameterization();
			spatparams.addParameter(TreeIndex.CACHE_SIZE_ID, cacheSize);
			spatparams.addParameter(TreeIndex.PAGE_SIZE_ID, blockSize);
			spatparams.addParameter(TreeIndex.FILE_ID, indexFile);
			
			// FloatVector is used for RStarTree
			RStarTree<FloatVector> rt = new RStarTree<FloatVector>(spatparams);
			// Used for break generic programming in bulk loading
			float[] fa = new float[dimension];
			FloatVector fv = new FloatVector(fa);

			// Use bulk loading to quickly build a RStarTree for S
			// In this case, we do not need to sort innerTable since
			// it is already sorted on Zorder and record ID at the start
			// of reduce stage.
			
			boolean sortLeafFile = false;
			try {
				rt.bulkLoad(fv, innerTable, innerSize, sortLeafFile, dimension);
			} catch (Exception e) {
				System.err.println("Bulkload throws exception : " + e.getMessage());
				System.exit(-1);	
			}

			reporter.progress();

			// 2) go through every record in outerTable
			FileReader frForR     = new FileReader(outerTable);
			BufferedReader brForR = new BufferedReader(frForR, bufferSize);

			EuclideanDistanceFunction<FloatVector> 
				dist = new EuclideanDistanceFunction<FloatVector>();

			int zvalOffset = 0;
			int ridOffset = zvalOffset + 1;

			while (true) 
			{
				String line = brForR.readLine();
				if (line == null) break;
				String parts[] = line.split(" +");
				
				String ridOfR = parts[ridOffset];
				String zvalueOfR = parts[zvalOffset];
				int[] coordOfR = Zorder.toCoord(zvalueOfR, dimension);
				float[] flCoordOfR = new float[dimension]; 
				for (int i = 0; i < dimension; i++)
					flCoordOfR[i] = coordOfR[i] * 1f;

				FloatVector fv1 = new FloatVector(flCoordOfR);
				List<DistanceResultPair<DoubleDistance>> ids =
					rt.kNNQuery(fv1, knn, (SpatialDistanceFunction<FloatVector, DoubleDistance>)dist);

				int cnt = 0;
				for (DistanceResultPair<DoubleDistance> res : ids) 
				{
					output.collect(NullWritable.get(), 
						new Text(ridOfR + " " + res.getID().toString() + 
						" " + res.getDistance().toString()
						)
					);
					//limit the number of candidates to knn
					cnt++;
					if (cnt == knn) break;
				}
				reporter.progress();
    		} // while

			brForR.close();
			frForR.close();

			// clear everything temporary
            try
			{
				Path tree_path = lDirAlloc.getLocalPathForWrite(prefix_dir, jobinfo);
				lfs.delete(tree_path, true);
				System.out.println(tree_path);
			} catch (IOException e) { e.printStackTrace(); }

		} // reduce

        public void close() throws IOException {
			//mos.close();
		}
						        
	} // Reducer

	// Customize the partitioner so that we use the random shift id for
	// partition 
	public static class RPhase1Partitioner 
	implements Partitioner<RPhase1Key, RPhase1Value> 
	{
		@Override
		public void configure(JobConf job) {}

		@Override
		public int getPartition(RPhase1Key key, RPhase1Value value, 
				int numPartitions) {
			// <zval, rid, groupID> 
			return key.getThird().get() % numPartitions;
		}
	}

	// Customize the map key comparator
	public static class RPhase1KeyComparator extends WritableComparator 
	{
		protected RPhase1KeyComparator() 
		{
			super(RPhase1Key.class, true);	
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			RPhase1Key rp1k1 = (RPhase1Key) w1;	
			RPhase1Key rp1k2 = (RPhase1Key) w2;

			int cmp = rp1k1.getFirst().compareTo(rp1k2.getFirst());
			if (cmp != 0) return cmp;	
			cmp = rp1k1.getSecond().compareTo(rp1k2.getSecond());
			return cmp;
		}
	}

	// Customize group comparator so that we can combine different key
	// values in a partition into one group
	public static class RPhase1GroupComparator extends WritableComparator 
	{
		protected RPhase1GroupComparator() 
		{
			super(RPhase1Key.class, true);	
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) 
		{
			RPhase1Key rp1k1 = (RPhase1Key) w1;	
			RPhase1Key rp1k2 = (RPhase1Key) w2;

			int cmp = rp1k1.getThird().compareTo(rp1k2.getThird()); 
			return cmp;
		}
	}
  
	static int printUsage() 
	{
		System.out.println(
			"NPhase1 [-m <maps>] [-r <reduces>] [-p <numberOfPartitions>] " 
			+ "[-d <dimension>] [-k <knn>] [-b <blockSize(#records) for R>] " 
			+ "<input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
  
	/**
	 * The main driver for phase1 of H-BRJ algorithm.
	 * Invoke this method to submit the map/reduce job.
	 * @throws IOException When there is communication problems with the 
	 *                     job tracker.
	 */
	public int run(String[] args) throws Exception 
	{
		JobConf conf = new JobConf(getConf(), RPhase1.class);
		conf.setJobName("RPhase1");

		conf.setOutputKeyClass(RPhase1Key.class);
		conf.setOutputValueClass(RPhase1Value.class);

		conf.setPartitionerClass(RPhase1Partitioner.class);
		conf.setOutputKeyComparatorClass(RPhase1KeyComparator.class);
		conf.setOutputValueGroupingComparator(RPhase1GroupComparator.class);
		
		conf.setMapperClass(MapClass.class);        
		conf.setReducerClass(Reduce.class);
		
		int numberOfPartition = 2;
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
/*				} else if ("-sj".equals(args[i])) {
					self_join = Boolean.parseBoolean(args[++i]);
					System.out.printf("Self_join is %s \n", self_join);	
					conf.set("self_join", Boolean.toString(self_join));
*/				} else if ("-b".equals(args[i])) {
					int block1 = Integer.parseInt(args[++i]);
					//conf.setInt("block", Integer.parseInt(args[++i]));
					conf.setInt("block", block1);
					System.out.printf("block is %d \n", block1);
				} else {
					other_args.add(args[i]);
		    	}
				conf.setNumReduceTasks(numberOfPartition * numberOfPartition);
				System.out.printf("The number of reducers are : %d\n", (numberOfPartition * numberOfPartition));
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " + args[i-1]);
				return printUsage();
			}
		}

		// Make sure there are exactly 3 parameters left.
		if (other_args.size() != 3) 
		{
		  System.out.println("ERROR: Wrong number of parameters: " + other_args.size() + " instead of 3.");
		  return printUsage();
		}

		FileInputFormat.setInputPaths(conf, other_args.get(0));
		System.out.println("Add R to the input path");
		FileInputFormat.addInputPaths(conf, other_args.get(1));
		System.out.println("Add S to the input path");
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(2)));

		JobClient.runJob(conf);

		return 0;
	}
  
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RPhase1(), args);
		System.exit(res);
	}
} // RPhase1
