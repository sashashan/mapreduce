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
import java.net.URI;
import java.lang.Long;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.DFSClient;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

/**
 * Phase1 of Hadoop zvalue KNN Join (H-zKNNJ).
 */
public class BPhase1 extends Configured implements Tool 
{
	BPhase1() {
		System.out.println("BPhase1");	
	}

	public static class MapClass extends MapReduceBase
		implements Mapper<LongWritable, Text, TextBytePair, BPhase1Value> 
	{
		private int numOfPartition = 0;
		private int scale = 1000;
		private int nr = 1000; 
		private int ns = 1000;     
		private double epsilon = 0.5; 
		private double sampleRate = 20;
		private double sampleRateOfR = 20;
		private double sampleRateOfS = 20;
		private boolean partitionOnR = false;
		private int knn = 3;
		private int dimension;
		private int shift;
		private int fileId = 0;
		private int[][] shiftvectors;
		private String inputFile = null;
		private Random r;
		private MultipleOutputs mos;
		private Reporter myReporter;
		private Path[] localFiles;

		public void configure(JobConf job) 
		{
			inputFile = job.get("map.input.file");
			shift = Integer.valueOf(job.get("shift"));
			dimension = Integer.valueOf( job.get("dimension"));
			numOfPartition = Integer.valueOf(job.get("numOfPartition"));
			nr = Integer.valueOf(job.get("Rsize"));
			ns = Integer.valueOf(job.get("Ssize"));
			System.out.println(ns);
			epsilon = Double.valueOf(job.get("epsilon"));
			System.out.println(epsilon);
			knn = Integer.valueOf(job.get("knn"));

			sampleRateOfR = 1 / (epsilon * epsilon * nr);	
			sampleRateOfS = 1 / (epsilon * epsilon * ns);

			r = new Random();
			
			if (sampleRateOfR > 1) sampleRateOfR = 1;		
			if (sampleRateOfS > 1) sampleRateOfS = 1;		

			if (sampleRateOfR * nr < 1) {
				System.out.printf("Increase sampling rate of R :  %d\n", sampleRateOfR);
				System.exit(-1);	
			}

			if (sampleRateOfS * ns < 1) {
				System.out.printf("Increase sampling rate of R :  %d\n", sampleRateOfS);
				System.exit(-1);	
			}

			if (inputFile.indexOf("outer") != -1)  
				fileId = 0;
			else if (inputFile.indexOf("inner") != -1)
				fileId = 1;
			else {
				System.out.println("Input filename error!");
				System.exit(-1);
			}

			// Grab random shift vector from distributed cache 
			try {
				localFiles = DistributedCache.getLocalCacheFiles(job);	
			} catch (IOException e)	{
				System.err.println("Caught exception while getting" + 
						" distributed cache files: ");
			}

			shiftvectors = new int[shift][dimension];
			try {
				BufferedReader br = new BufferedReader( 
					new FileReader( localFiles[0].toString() ), 1024);
				int j = 0;
				while(true) {
					String line = br.readLine();
					if (line == null)
						break;
					String[] parts = line.split(" ");	
					for (int i = 0 ; i < dimension; i++)
						shiftvectors[j][i] = Integer.valueOf(parts[i]);
					j++;	
				}
				br.close();
			} catch (IOException e) {
				System.err.println("Caught exception while reading" + 
						" distributed cache files: ");
			}
			mos = new MultipleOutputs(job);
		} // configure

		/**
		* In map stage, the algorithm generates random shifts for the original
		* datasets(R and S) and save these shifts on HDFS; additonally it 
		* samples small datasets and pass them to the reduce stage. 
		*/
		public void map(LongWritable key, Text value, 
			OutputCollector<TextBytePair, BPhase1Value> output, 
			Reporter reporter) throws IOException {

			myReporter = reporter;

			String line = value.toString();
			String zval = null;
			char ch = ' ';
			int pos = line.indexOf(ch);
			String id = line.substring(0, pos);
			String rest = line.substring(pos + 1, line.length()).trim(); 
			String[] parts = rest.split(" +");
			float[] coord = new float[dimension];
			for (int i = 0; i < dimension; i++) {
				coord[i] = Float.valueOf(parts[i]);
			}

			// generate m random shift copies	
			for (int i = 0; i < shift; i++)	
			{
				float[] tmp_coord = new float[dimension];
				// Scale up coordinates and add random shift vector
				int[] converted_coord = new int[dimension];
				for (int k = 0; k < dimension; k++) 
				{
					tmp_coord[k] = coord[k];
					// To prevent precision loss, we need to scale up
					// the part behide the decimal point to integer.
					converted_coord[k] = (int) tmp_coord[k]; // Get integer part
					tmp_coord[k] -= converted_coord[k];  // Get fractional part
					converted_coord[k] *= scale;         // Scale integer part
					converted_coord[k] += (tmp_coord[k] * scale); 
					if (i != 0)   //for shift 0 we use the original setting
						converted_coord[k] += shiftvectors[i][k]; // Add shift
				}

				zval = Zorder.valueOf(dimension, converted_coord);
		
				if (fileId == 0) 
					sampleRate = sampleRateOfR;
				else if (fileId == 1)
					sampleRate = sampleRateOfS;
				else {
					System.out.println("Wrong source file!");
					System.exit(-1);	
				}

				boolean sampled = false;	
				if (r.nextDouble() < sampleRate)
					sampled = true;
				if (sampled) {
					output.collect(new TextBytePair(zval, (byte)i),
					new BPhase1Value(zval, Integer.valueOf(id), (byte)fileId));
					/*
						mos.getCollector("mytest", reporter).collect(
							new Text(zval + " " + Integer.toString(i)), 
							new Text(zval + " " + id)	
						);
					*/
				}

				Text mapResKey = new Text(Integer.toString(i));
				String mapResValueStr = zval + " " +  id + " " +  Integer.toString(fileId) + " " + Integer.toString(i); 
				Text mapResValue = new Text(mapResValueStr);
			
				if (fileId == 0)	
					mos.getCollector("Rconverted" + Integer.toString(i), 
						reporter).collect(NullWritable.get(), mapResValue);
				else if (fileId == 1)
					mos.getCollector("Sconverted" + Integer.toString(i), 
						reporter).collect(NullWritable.get(), mapResValue);
				else {
					System.out.println("Unknown input file source");
					System.exit(-1);
				}
			}
		}

		public void close() throws IOException {
			mos.close();	
		}
	}
 
	/**
	 * The algorithm estimates partitioning ranges in the reduce stage.	
	 */
	public static class Reduce extends MapReduceBase implements 
	Reducer<TextBytePair, BPhase1Value, Text, Text> 
	{
		int shift;
		int numOfPartition = 0;
		int nr = 1000; 
		int ns = 1000; 
		int dimension;
		float epsilon = 0.5f; 
		float sampleRateOfR;
		float sampleRateOfS;
		int knn = 3;
		boolean partitionOnR = false;
		boolean selfjoin = false;
		MultipleOutputs mos; 

		public void configure(JobConf job) 
		{
			knn = Integer.valueOf(job.get("knn"));
			shift = Integer.valueOf(job.get("shift"));     
			numOfPartition = Integer.valueOf(job.get("numOfPartition")); 
			nr = Integer.valueOf(job.get("Rsize"));
			ns = Integer.valueOf(job.get("Ssize"));
			dimension = Integer.valueOf( job.get("dimension"));
			epsilon = Float.valueOf(job.get("epsilon"));
			sampleRateOfR = 1 / (epsilon * epsilon * nr);	
			sampleRateOfS = 1 / (epsilon * epsilon * ns);	
			partitionOnR = Boolean.valueOf(job.get("partitionOnR"));
			selfjoin = Boolean.valueOf(job.get("selfjoin"));
		
			if (sampleRateOfR > 1) sampleRateOfR = 1;		
			if (sampleRateOfS > 1) sampleRateOfS = 1;

			mos = new MultipleOutputs(job);
		}


		/**
		 * To calculate the index of the estimator for i-th q-quantiles 
		 * in a given sampled data set (size).
		 */
		// !! sampleRate is different for R and S
		public int getEstimatorIndex(int i, int size, float sampleRate, 
		int numOfPartition) 
		{
			double iquantile = (i * 1.0 / numOfPartition);
			int orgRank = (int) Math.ceil((iquantile * size));
			int estRank = 0;
			
			int val1  = (int) Math.floor(orgRank * sampleRate);
			int val2  = (int) Math.ceil(orgRank * sampleRate);

			int est1 = (int) (val1 * (1 / sampleRate));
			int est2 = (int) (val2 * (1 / sampleRate));

			int dist1 = (int) Math.abs(est1 - orgRank);
			int dist2 = (int) Math.abs(est2 - orgRank);

			if (dist1 < dist2)
				estRank = val1;
			else
				estRank = val2;
/*
			double iquantile = (i * 1.0 / numOfPartition);
			int orgRank = Math.ceil(iquantile * size);
			int est1  = orgRank	 
*/
			return estRank;
		}	

		public void reduce(TextBytePair key, Iterator<BPhase1Value> values,
		OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			ArrayList<String> RtmpList = new ArrayList<String>();
			ArrayList<String> StmpList = new ArrayList<String>();
			ArrayList<Integer> RridList = new ArrayList<Integer>();
			ArrayList<Integer> SridList = new ArrayList<Integer>();

			String sidStr = key.getSecond().toString();

			// value format <zval, rid, src>
			int zOffset = 0;
			int ridOffset = 1;
			int srcOffset = 2;
			while (values.hasNext()) 
			{
				String line = values.next().toString();
				String[] parts = line.split(" +");
				if (Integer.valueOf(parts[srcOffset]) == 0)	 {
					RtmpList.add(parts[zOffset]);
				} else {
					StmpList.add(parts[zOffset]);
				}
			} 

			int Rsize = RtmpList.size(); 
			int Ssize = StmpList.size();

			if (partitionOnR) {
				String q_start = "";
				int len = Zorder.maxDecDigits(dimension);
				q_start = Zorder.createExtra(len);

				for (int i = 1; i <= numOfPartition; i++) {

					int estRank = getEstimatorIndex(i, nr, sampleRateOfR,
							numOfPartition);	
					if (estRank - 1 >= Rsize)
						estRank = Rsize;

					String q_end;
					if (i == numOfPartition) {
						q_end = Zorder.maxDecString(dimension);	
					} else
						q_end = RtmpList.get(estRank - 1);
    
					mos.getCollector("Rrange" + sidStr, reporter).collect(
							new Text(" "), new Text(q_start + " " + q_end));
    
					int low;
					if (i == 1) 
						low = 0;
					else {
						int newKnn = 
							(int)Math.ceil((double) knn / (epsilon*epsilon*ns));
						//newKnn = knn;
						low = Collections.binarySearch(StmpList, q_start);
						if (low < 0)
							low = -low - 1;
						if ((low - newKnn) < 0)
							low = 0;
						else
							low -= newKnn;
					}

					String s_start;
					if (i == 1) {
						len = Zorder.maxDecDigits(dimension);
						s_start = Zorder.createExtra(len);
					} else
						s_start = StmpList.get(low);
					
					int high;
					if (i == numOfPartition) {
						high = Ssize - 1;	
					} else {
						int newKnn = (int)Math.ceil(
							(double) knn / (epsilon*epsilon*ns));
					//	newKnn = knn;
                    
						high = Collections.binarySearch(StmpList, q_end);
						if (high < 0)
							high = -high - 1;
						if ((high + newKnn) > Ssize -1)
							high = Ssize -1;
						else 
							high += newKnn; 
					}
                    
					String s_end;
					if (i == numOfPartition) {
						s_end = Zorder.maxDecString(dimension);	
					} else { 
						s_end = StmpList.get(high);
					}
					
					mos.getCollector("Srange" + sidStr, reporter).collect(
							new Text(" "), new Text(s_start + " " + s_end));

					q_start = q_end; 
				} // for
			} else {
				String q_start = StmpList.get(0);
				int lowEstRank = 1;
				for (int i = 1; i <= numOfPartition; i++) {
					//Determine the partition range for S
					int estRank = getEstimatorIndex(i, ns, sampleRateOfS,
							numOfPartition);	
    
					if (estRank - 1 >= Ssize)
						estRank = Ssize;

					int low  = lowEstRank;
					int high = estRank;
					// Need to duplicate some points here
					if (i == 1) {
						lowEstRank = 1;
						low = lowEstRank;
					} else {
						int newKnn = 
							(int) Math.ceil((double) knn / (epsilon*epsilon*ns));
						//newKnn = knn;

						lowEstRank -= newKnn; 
						if (lowEstRank <= 0)
							lowEstRank = 1;
					}
	
					if (i == 1) {
						int len = Zorder.maxDecDigits(dimension);
						q_start = Zorder.createExtra(len);
					} else
						q_start = StmpList.get(lowEstRank - 1);

					if (i == numOfPartition) {
						estRank = Ssize;
						high = estRank;
					} else {
						int newKnn = 
							(int) Math.ceil((double) knn / (epsilon*epsilon*ns));
						//newKnn = knn;

						estRank += newKnn;
						if (estRank > Ssize)
							estRank = Ssize;
					}

					String q_end;
					if (i == numOfPartition) {
						q_end = Zorder.maxDecString(dimension);	
					} else
						q_end = StmpList.get(estRank - 1);

					mos.getCollector("Srange" + sidStr, reporter).collect(
							new Text(" "), new Text(q_start + " " + q_end
								+ " " + Integer.toString(lowEstRank) + 
								" " + Integer.toString(estRank)
								));

					String r_start, r_end;
                    
					if (i == 1)
						r_start = q_start;
					else
						r_start = StmpList.get(low - 1);
                    
					 if (i == numOfPartition)
						 r_end = q_end;
					else
					 	r_end = StmpList.get(high - 1);
                    
					mos.getCollector("Rrange" + sidStr, reporter).collect(
						new Text(" "), new Text(r_start + " " + r_end));

					//lowEstRank = estRank;
					lowEstRank = high;
					q_start = q_end; 
				} //for
			} // else partitionOnS

		} // reduce

		public void close() throws IOException {
			mos.close();	
		}
	} // Reducer
 
	// Customize the partitioner so that we use the random shift id for
	// partition 
	public static class SecondPartitioner 
		implements Partitioner<TextBytePair, BPhase1Value> {

		@Override
		public void configure(JobConf job) {}

		@Override
		public int getPartition(TextBytePair key, BPhase1Value value, 
			int numPartitions) {
			return key.getSecond().get() % numPartitions;
		}
	}
   	
	// Customize the map key comparator
	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(TextBytePair.class, true);	
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			TextBytePair tp1 = (TextBytePair) w1;	
			TextBytePair tp2 = (TextBytePair) w2;

			int cmp = tp1.getSecond().compareTo(tp2.getSecond());
			if( cmp != 0 ) return cmp;
                        
			cmp = tp1.getFirst().toString().compareTo(tp2.getFirst().toString()); 

			return cmp;
		}
	}

	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(TextBytePair.class, true);	
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			TextBytePair tp1 = (TextBytePair) w1;	
			TextBytePair tp2 = (TextBytePair) w2;
			int cmp = tp1.getSecond().compareTo(tp2.getSecond()); 
			return cmp;
		}
	}
	
	public void genRandomShiftVectors(JobConf job, String filename, 
		int dimension, int shift) throws IOException {
  
		Random r = new Random(); 
		int[][] shiftvectors = new int[shift][dimension];

		// Generate random shift vectors
		for (int i = 0; i < shift; i++) {
			shiftvectors[i] = Zorder.createShift(dimension, r, true);
		}
		OutputStreamWriter osw = getWriter(job, filename);

		// Save random shift vectors in a HDFS file	
		for (int j = 0; j < shift; j ++)  {
			String shiftVector = "";
			for (int k = 0; k < dimension; k++) 
				shiftVector += Integer.toString(shiftvectors[j][k]) + " ";

			osw.write(shiftVector + "\n");
		}
		osw.close();
	}

    public static OutputStreamWriter getWriter( JobConf job, final String file ) 
		throws IOException {
          DFSClient dfs = new DFSClient( job );
          return new OutputStreamWriter( 
				  new BufferedOutputStream( dfs.create( file , true ) ) );
	}

	static int printUsage() {
		System.out.println(
			"BPhase1 -m <maps> -r <reduces> -s <numberOfShifts> "
			+ "-p <numberOfPartitions>  -nr <numberOfRecordsFromR> " 
			+ "-ns <numberOfRecordsFromS> -e <epsilon> -d <dimension> " 
			+ "-k <knn> -pr <partitionOnR> -c <cluster_config> "
			// + "-sj <self_join> " 
			+ "-outer <R> -inner <S> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
	  return -1;
	}

	public int run(String[] args) throws Exception 
	{
	    int numOfPartition = 0;
		boolean selfjoin = false;
	    int nr = 1000; 
	    int ns = 1000; 
	    double epsilon = 0.5; 
	    boolean partitionOnR = true; 
	    int knn = 3;
	    int dimension = 2;
	    int shift = 3;
		String clusterConfiguration = null;

		JobConf conf = new JobConf(getConf(), BPhase1.class);
		conf.setJobName("BPhase1Join");

		// Sort and group based on different features
		conf.setPartitionerClass(SecondPartitioner.class);
		conf.setOutputKeyComparatorClass(KeyComparator.class);
		conf.setOutputValueGroupingComparator(GroupComparator.class);

		// For mapper
		conf.setOutputKeyClass(TextBytePair.class);
		conf.setOutputValueClass(BPhase1Value.class);
	
		// Mapper and Reducer class	
		conf.setMapperClass(MapClass.class);        
		conf.setReducerClass(Reduce.class);
		
		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					i++;
					//conf.setNumMapTasks(Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					// Never really used, set by the number of shift
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else if ("-s".equals(args[i])) {
					shift = Integer.parseInt(args[++i]);
					conf.set("shift", Integer.toString(shift));
					conf.setNumReduceTasks(shift);
				} else if ("-p".equals(args[i])) {
					numOfPartition = Integer.parseInt(args[++i]);
					conf.set("numOfPartition", Integer.toString(numOfPartition)); 
				} else if ("-nr".equals(args[i])) {
					nr = Integer.parseInt(args[++i]);
					conf.set("Rsize", Integer.toString(nr)); 
				} else if ("-ns".equals(args[i])) {
					ns = Integer.parseInt(args[++i]);
					conf.set("Ssize", Integer.toString(ns)); 
				} else if ("-e".equals(args[i])) {
					epsilon = Double.parseDouble(args[++i]);
					System.out.println(epsilon); 
					conf.set("epsilon", Double.toString(epsilon)); 
				} else if ("-d".equals(args[i])) {
					dimension = Integer.parseInt(args[++i]);
					System.out.println("Dimension is " + dimension); 
					conf.set("dimension", Integer.toString(dimension)); 
				} else if ("-k".equals(args[i])) {
					knn = Integer.parseInt(args[++i]);
					conf.set("knn", Integer.toString(knn)); 
				} else if ("-pr".equals(args[i])) {
					partitionOnR = Boolean.parseBoolean(args[++i]);
					conf.set("partitionOnR", Boolean.toString(partitionOnR)); 
				} else if ("-c".equals(args[i])) {
					clusterConfiguration = args[++i];
					conf.set("clusterconfiguration", clusterConfiguration); 
/*				} else if ("-sj".equals(args[i])) {
					selfjoin = Boolean.parseBoolean(args[++i]);
					conf.set("selfjoin", Boolean.toString(selfjoin)); 
*/				} else if ("-outer".equals(args[i])) {
					String outer = args[++i];
					FileInputFormat.setInputPaths(conf, outer); // Outer
				} else if ("-inner".equals(args[i])) {
					String inner = args[++i];
					FileInputFormat.addInputPaths(conf, inner); // Inner
				} else if ("-o".equals(args[i])) {
					String output_dir = args[++i];
					FileOutputFormat.setOutputPath(conf, new Path(output_dir));
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

		// Define output files for map stage
		for (int i = 0; i < shift; i++) {
			MultipleOutputs.addNamedOutput(conf, "Rconverted" + 
					Integer.toString(i), 
					TextOutputFormat.class,	NullWritable.class, Text.class);
			MultipleOutputs.addNamedOutput(conf, "Sconverted" +
					Integer.toString(i), 
					TextOutputFormat.class, NullWritable.class, Text.class);
			MultipleOutputs.addNamedOutput(conf, 
					"Rrange" + Integer.toString(i),
					TextOutputFormat.class, Text.class, Text.class);
			MultipleOutputs.addNamedOutput(conf, 
					"Srange" +  Integer.toString(i), 
					TextOutputFormat.class, Text.class, Text.class);
		}

		MultipleOutputs.addNamedOutput(conf, "shiftvector", 
			TextOutputFormat.class, Text.class, Text.class);

		String filename = "/user/hadoop/" + clusterConfiguration + "/RandomShiftVectors";
		System.out.println(filename);
	    genRandomShiftVectors(conf, filename, dimension, shift);

		DistributedCache.addCacheFile(new URI(filename), conf);	
		//System.out.printf("shift %d partsize %d\n", shift, numOfPartition);

		JobClient.runJob(conf);
		return 0;
	}
  
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BPhase1(), args);
		System.exit(res);
	}

}
