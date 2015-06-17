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

import com.mellowtech.collections.BPlusTree;
import com.mellowtech.collections.KeyValue;
import com.mellowtech.collections.*;
import com.mellowtech.disc.*;

import java.io.*;
import java.util.*;
import java.net.URI;

import java.lang.Long;
import java.lang.Character;
import java.lang.String;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.net.NetworkTopology;


/**
 * Phase2 of Hadoop zvalue based KNN Join (H-zKNNJ).
 */
public class BPhase2 extends Configured implements Tool {
	public static int bufInLength = 8 * 1024 * 1024;

	BPhase2() {
		System.out.println("BPhase2");	
	}

	public static int parseFileName(String fileName) {
		int ret = 0;
		int pos = 0;

		pos = fileName.lastIndexOf("/");
		String realName = fileName.substring(pos + 1, fileName.length());

		// Assume the fileName has a form of CCCCCC#, for example Rrange0.
		String relation = realName.substring(0, 1);
		String shiftCopy = realName.substring(6, 7);
		if (relation.compareTo("R") == 0)
			//ret = Integer.valueOf(shiftCopy).intValue() + 1;
			ret = Integer.valueOf(shiftCopy) + 1;
		else if (relation.compareTo("S") == 0)
			//ret = -(Integer.valueOf(shiftCopy).intValue() + 1);
			ret = -(Integer.valueOf(shiftCopy) + 1);
		else {
			System.out.println(relation + "@" + realName);
			System.out.println("Incorrect DC file name!!!");
			System.exit(-1);
		}

		return ret;
	}

	public static void fillMark(int id, ArrayList<ArrayList<String>> mark,
		String fileName, int numOfPartition) {

		ArrayList<String> list = mark.get(id);
		try {
			FileReader fr = new FileReader(fileName);
			BufferedReader br = new BufferedReader(fr, bufInLength);
			
			while(true) {
				String line = br.readLine();
				if (line == null)
					break;
				list.add(line.trim());
			}
	    
			if (list.size() != numOfPartition) {
				System.out.println(id);	
				System.out.println("List size is wrong");	
				System.out.println(list.size());	
				System.exit(-1);	
			}
			
			br.close();
			fr.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Exception @fillMark");
            System.exit(-1);
        }
	} // fillMark

	public static class MapClass extends MapReduceBase
		implements Mapper<LongWritable, Text, BPhase2Key, BPhase2Value> {
   
		private int bufInLength = 8 * 1024 * 1024;
		private int fileId = 0;
		private String inputFile = null;
		private String mapTaskId;
		private int dimension = 2;
		private int shift = 3;
		private int numOfPartition = 3;
        
		private	Text mapKey = null;  
		private	Text mapValue = null;
		private	char ch = ' ';      

		private Path[] localFiles;
		private MultipleOutputs mos;

		private ArrayList<ArrayList<String>> Rmark = 
			new ArrayList<ArrayList<String>>();
		private ArrayList<ArrayList<String>> Smark = 
			new ArrayList<ArrayList<String>>();



		public ArrayList<String> getPartitionId(String z, String src, 
			String sid) {

			String ret = null;
			int sidInt = Integer.valueOf(sid);
			ArrayList<String> mark = null;
			ArrayList<String> idList = new ArrayList<String>(numOfPartition);

			// 0 - R from Outer 1 - S from inner
			if (src.compareTo("0") == 0)
				mark = Rmark.get(sidInt);
			else if (src.compareTo("1") == 0)	
				mark = Smark.get(sidInt);
			else {
				System.out.println(src);
				System.out.println("Unknown source for input recrod !!!");
				System.exit(-1);	
			}
			
			// Partition is ordered from 0
			int i = 0;
			for (; i < numOfPartition; i++) {
				String range = mark.get(i);
				String[] parts = range.split(" +");
				String low = parts[0];
				String high = parts[1];

				// Check z against all partitions, this is a must
				if (z.compareTo(low) >=0 && z.compareTo(high) <= 0) {
					ret = Integer.toString(i); 		
					idList.add(ret);
					//break;
				}
			}

			//return ret;
			return idList;
		}

		public void configure(JobConf job) {
			inputFile = job.get("map.input.file");
			mapTaskId = job.get("mapred.task.id");
			shift = Integer.valueOf(job.get("shift"));
			numOfPartition = Integer.valueOf(job.get("numOfPartition"));

			try {
				localFiles = DistributedCache.getLocalCacheFiles(job);
			} catch (IOException e) {
				System.err.println("Caught exception while getting" +
					" distributed cache files: ");
			}
			
			for (int i = 0; i < shift; i++) {
				Rmark.add(new ArrayList<String>());
				Smark.add(new ArrayList<String>());
			}

			// Multiple files in DC, range files start at index of 2
			int rangeFileOffset = 1;
			for (int i = 0; i < shift * 2; i++) {
				String fname = localFiles[i + rangeFileOffset].toString();
				int val = parseFileName(fname);
				
				if (val > 0)
					fillMark(val - 1, Rmark, fname, numOfPartition);
				else
					fillMark(-val - 1, Smark, fname, numOfPartition);
			}

			mos = new MultipleOutputs(job);
		}

		
		public void map(LongWritable key, Text value, 
			OutputCollector<BPhase2Key, BPhase2Value> output, Reporter reporter) 
			throws IOException {
	
			String line = value.toString().trim();
			//int pos = line.indexOf('\t');
			//line = line.substring(pos + 1, line.length()).trim(); 

			// Input record format <z, rid, coord, src, sid>
			String[] parts = line.split(" +");
			int zOffset, ridOffset, coordOffset, srcOffset, sidOffset;
			zOffset = 0;
			ridOffset = zOffset + 1;
			srcOffset = ridOffset + 1;
			sidOffset = srcOffset + 1;

/*
			if (numOfPartition == 1) {
				//In this case we only use two nodes
				int intSid = Integer.valueOf(parts[sidOffset]); 
				int groupKey = intSid;
                
				// Key format <zvalue, groupid>
				BPhase2Key bp2k = new BPhase2Key(parts[zOffset], (byte)groupKey);
				BPhase2Value bp2v = new BPhase2Value(
					parts[zOffset], Integer.valueOf(parts[ridOffset]),
						Byte.valueOf(parts[srcOffset]));
				output.collect(bp2k, bp2v);
			} else {
*/

			// Figure out to which partition range the record belong to.
			ArrayList<String> pidList = getPartitionId(parts[zOffset], 
							parts[srcOffset], parts[sidOffset]);
			if (pidList.size() == 0) {
				System.out.println("Cannot get pid");
				System.exit(-1);
			}

			int i = 0;
			for (; i < pidList.size(); i++) {
				String pid  = pidList.get(i);
				int intSid = Integer.valueOf(parts[sidOffset]); 
				int intPid = Integer.valueOf(pid);
				int groupKey = intSid * numOfPartition + intPid;
				
				// ((zvalue, groupid), (zvalue, rid, src))
				BPhase2Key bp2k 
					= new BPhase2Key(parts[zOffset], (byte)groupKey);

				BPhase2Value bp2v = new BPhase2Value(parts[zOffset], 
					Integer.valueOf(parts[ridOffset]), Byte.valueOf(
						parts[srcOffset]));

				output.collect(bp2k, bp2v);
			}
//			} // if numOfPartition = 1
		} // map

		public void close() throws IOException {
			mos.close();	
		}
	} // Mapper

	public static class Reduce extends MapReduceBase
		implements Reducer<BPhase2Key, BPhase2Value, Text, Text> {

		private String reduceTaskId;
		private String inputFile;
		private static int knn = 3;
		private static int knnFactor = 4;
		private int shift = 3;
		private int numOfPartition;
		private int dimension = 3;

		// Parameters for BPlus tree
		//private int indexBlockSize = 1024 * 4; // 4k size
		//private int valueBlockSize = 1024 * 4;
		private int indexBlockSize = 1024 * 32; // 4k size
		private int valueBlockSize = 1024 * 32;

		// Variables for dealing with storing intermeidate files
		// Will the transient store directory for maps and reduces ok?...
		private LocalDirAllocator lDirAlloc =
			new LocalDirAllocator("mapred.local.dir");

		private FSDataOutputStream out;
		private MultipleOutputs mos;
		private FileSystem localFs;	
		private FileSystem lfs;	
		private Path file1;
		private Path file2;
		private int bufInLength = 8 * 1024 * 1024;
		private int bufferSize = 8 * 1024 * 1024;

		private int[][] shiftvectors;	
		private Path[] localFiles;
		int zOffset, ridOffset, coordOffset, srcOffset, sidOffset;

		private int[] counters;
		private ArrayList<ArrayList<String>> Rmark =  // Range information of R
			new ArrayList<ArrayList<String>>();
		private ArrayList<ArrayList<String>> Smark =  // Range information of S
			new ArrayList<ArrayList<String>>();

		// key type and value type for B+ tree
		CBString keyType = new CBString();
		CBInt valueType = new CBInt();

		private Configuration jobinfo;

		public void getRandomShiftVectors(String file, int[][] shiftvectors) {
			try {
				FileReader fr = new FileReader(file);
				BufferedReader br = new BufferedReader(fr, 1024);
				int i = 0;
				while(true) {
					String line = br.readLine();
					if (line == null)
						break;
					String[] parts = line.split(" ");
					for (int j = 0; j < dimension; j++) {
						System.out.printf("%d -- %d\n", i, j);
						shiftvectors[i][j] = Integer.valueOf(parts[j]);	
					}
					i++;	
				}
				br.close();
				fr.close();
			} catch (IOException e) {
				System.out.println("Reading random shift vectors error!!!");	
			}	
		}

		public void configure(JobConf job) {
			reduceTaskId = job.get("mapred.task.id");
			inputFile = job.get("map.input.file");
			shift = Integer.valueOf(job.get("shift"));
			knn = Integer.valueOf(job.get("knn"));
			numOfPartition = Integer.valueOf(job.get("numOfPartition"));
			dimension = Integer.valueOf(job.get("dimension"));

			try {
				localFs = FileSystem.getLocal(job);
			} catch (IOException e) {
				e.printStackTrace();
			}

			lfs = ((LocalFileSystem) localFs).getRaw();
			jobinfo = job;

			zOffset = 0;
			ridOffset = zOffset + 1;
			srcOffset = ridOffset + 1;

			// Access DC files having vector information
			try {
				localFiles = DistributedCache.getLocalCacheFiles(job);
			} catch (IOException e) {
				System.err.println("Caught exception while getting" +
					" distributed cache files: ");
			}
			//System.out.printf("%d , %d\n", shift, dimension);
			//System.exit(-1);
			shiftvectors = new int[shift][dimension];
			getRandomShiftVectors(localFiles[0].toString(), shiftvectors);

			try {
				localFiles = DistributedCache.getLocalCacheFiles(job);
			} catch (IOException e) {
				System.err.println("Caught exception while getting" +
					" distributed cache files: ");
			}
	
			counters = new int[2];
			counters[0] = counters[1] = 0;

			for (int i = 0; i < shift; i++) {
				Rmark.add(new ArrayList<String>());
				Smark.add(new ArrayList<String>());
			}

			// Multiple files in DC, range files start at index of 2
			int rangeFileOffset = 1;
			for (int i = 0; i < shift * 2; i++) {
				String fname = localFiles[i + rangeFileOffset].toString();
				int val = parseFileName(fname);
				
				if (val > 0)
					fillMark(val - 1, Rmark, fname, numOfPartition);
				else
					fillMark(-val - 1, Smark, fname, numOfPartition);
			}
			
			mos = new MultipleOutputs(job);
		}

		
		public void reduce(BPhase2Key key, Iterator<BPhase2Value> values,
			OutputCollector<Text, Text> output, Reporter reporter) 
			throws IOException {

			int groupId = (int)key.getSecond().get();
			int sid = groupId / numOfPartition;  // Shift id
			int pid = groupId % numOfPartition;  // Partition id
			String prefix_dir = "hzknnj" + "-"  + groupId;
			
			// Create seperate local files for different key value
			try {

				file1 = lDirAlloc.getLocalPathForWrite(prefix_dir + "/" 
					+ "outer", jobinfo); 
				out = lfs.create(file1);
				out.close();

				file2 = lDirAlloc.getLocalPathForWrite(prefix_dir + "/" 
					+ "inner", jobinfo);
				out = lfs.create(file2);
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			// Create local files to save records from R and S
			String outerTable = file1.toString();
			String innerTable = file2.toString();
			FileWriter fwForR = new FileWriter(outerTable);
			BufferedWriter bwForR = new BufferedWriter(fwForR, bufferSize);

			FileWriter fwForS	= new FileWriter(innerTable);
			BufferedWriter bwForS = new BufferedWriter(fwForS, bufferSize);

			// value format <zvalue, rid, src>
			while (values.hasNext()) {
				String line = values.next().toString();

				String[] parts = line.split(" +");
				String zvalue = parts[zOffset];
				String rid = parts[ridOffset];
				String src = parts[srcOffset];
				int srcId = Integer.valueOf(src);

				String tmpRecord = zvalue + " " + rid + "\n";
				if (srcId == 0)  // from R
					bwForR.write(tmpRecord);
				else if (srcId == 1) // from S
					bwForS.write(tmpRecord);
				else {
					System.out.println(srcId);
					System.out.println("The record has an unknown source!!");
					System.exit(-1);	
				}
			}  // while 

			bwForR.close();
			fwForR.close();
			bwForS.close();
			fwForS.close();


			reporter.progress();

			// The following part can be replaced by a in memory method
			// in the case of the datasets can be fitted into memory.	

//if (1 < 2) {

			BPlusTree bpt = new BPlusTree(innerTable, keyType, valueType,
					valueBlockSize, indexBlockSize);
			bpt.setTreeCache(32* 1024 * 1024, 32 * 1024 * 1024);

			int flag = 0; // 0 for CBString, CBInt
			bpt.createIndexBL(innerTable, bufInLength, flag);
			bpt.save();

			FileReader frForR	= new FileReader(outerTable);
			BufferedReader brForR = new BufferedReader(frForR, bufInLength);
			
			float hashTableLoadFactor = 0.75f;
			knnFactor = 4;
			Reduce.knnFactor = 4;
			int hashTableCapacity =  
				(int) Math.ceil((knnFactor * knn ) / hashTableLoadFactor) + 1;

			LinkedHashMap<String, ArrayList<Integer>> coordLRUCache = 
			new LinkedHashMap<String, ArrayList<Integer>>(hashTableCapacity,
													hashTableLoadFactor, true)
			{
        		@Override
        		protected boolean removeEldestEntry(Map.Entry<String, 
					ArrayList<Integer>> eldest) {
          			return size() > Reduce.knnFactor * Reduce.knn;
				}	
	  		};

			int cnt = 0;
			boolean loop = true;
			while(loop) {
				String line = brForR.readLine();
				if (line == null) break;

				String[] parts = line.split(" +");
				String zval = parts[0];
				String rid = parts[1];
				int[] coord = Zorder.toCoord(zval, dimension);

				CBString searchKey = new CBString(zval);
				ArrayList<ArrayList<KeyValue>> knnList = 
					bpt.rangeSearch(new CBString(zval), knn);

				ArrayList<KnnRecord> knnListSorted = new ArrayList<KnnRecord>();
				Comparator<KnnRecord> krc = new KnnRecordComparator();
				for (ArrayList<KeyValue> l: knnList) {
					for (KeyValue e :l) {
					
						String zval2 = ((CBString) e.getKey()).getString();
						int rid2 = ((CBInt) e.getValue()).getValue();
						int[] coord2 = null;

						ArrayList<Integer> cachedCoord2 
												= coordLRUCache.get(zval2);

						if (cachedCoord2 == null) {
							coord2 = Zorder.toCoord(zval2, dimension);
							ArrayList<Integer> ai = 
								new ArrayList<Integer>(dimension);
							for (int i = 0; i < dimension; i++) {
								ai.add(coord2[i]);
							}
							coordLRUCache.put(zval2, ai);
						} else {
							//int[] coord2 = cacheCoord2.toArray();
							coord2 = new int[dimension];
							for (int i = 0; i < dimension; i++) 
								coord2[i] = cachedCoord2.get(i);
						}

						float dist = (float) 0.0;
						for (int i = 0; i < dimension; i++)	
							dist += (float) ((coord[i] - coord2[i]) * 
										(coord[i] - coord2[i]));

						KnnRecord kr = 
							new KnnRecord(rid2, (float)Math.sqrt(dist));
						knnListSorted.add(kr);
					}
				}

				Collections.sort(knnListSorted, krc); 

				for (int i = 0; i < knn; i++) {
					KnnRecord kr = knnListSorted.get(i);
					output.collect(new Text(rid), new Text(" " + kr.getRid() 
							+ " " + Float.toString(kr.getDist())));
				}

				if (cnt++ % 10000 == 0)
					reporter.progress();

			}
//	} // if (1 > 2)
			lfs.delete(file1, true);
			lfs.delete(file2, true);

		} // reduce

		public void close() throws IOException {
			mos.close();	
		}
	} // Reducer

	public static class SecondPartitioner 
		implements Partitioner<BPhase2Key, BPhase2Value> {

		@Override
		public void configure(JobConf job) {}

		@Override
		public int getPartition(BPhase2Key key, BPhase2Value value, 
				int numPartitions) {
			return key.getSecond().get() % numPartitions;
		}
	}
   	
	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(BPhase2Key.class, true);	
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			BPhase2Key bp2k1 = (BPhase2Key) w1;	
			BPhase2Key bp2k2 = (BPhase2Key) w2;

			int cmp = bp2k1.getSecond().compareTo(bp2k2.getSecond()); 
                        if( cmp != 0 ) return cmp;
			cmp = bp2k1.getFirst().toString().compareTo(
					bp2k2.getFirst().toString()); 

			return cmp;
		}
	}

	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(BPhase2Key.class, true);	
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			BPhase2Key bp2k1 = (BPhase2Key) w1;	
			BPhase2Key bp2k2 = (BPhase2Key) w2;
			int cmp = bp2k1.getSecond().compareTo(bp2k2.getSecond()); 

			return cmp;
		}
	}
 
	static int printUsage() {
		System.out.println(
			"BPhase2 -m <maps> -r <reduces> -s <numberOfShifts> "
			+ "-p <numberOfPartitions> -d <dimension> " 
			+ "-k <knn> -c <cluster_config> "
			//+ "-sj <self_join> " 
			+ "-outer <R> -inner <S> " 
			+ "<input> <output>");
	  ToolRunner.printGenericCommandUsage(System.out);
	  return -1;
	}
  
	public int run(String[] args) throws Exception {
		int shift = 3;
		int numOfPartition = 3;
		int dimension = 2;
		int knn = 3;
		String outer = null;
		String inner = null;	
		String clusterConfiguration = null;

		JobConf conf = new JobConf(getConf(), BPhase2.class);
		conf.setJobName("BPhase2Join");

		// Output key and value class for Mapper
		conf.setOutputKeyClass(BPhase2Key.class);
		conf.setOutputValueClass(BPhase2Value.class);

		conf.setPartitionerClass(SecondPartitioner.class);
		conf.setOutputKeyComparatorClass(KeyComparator.class);
		conf.setOutputValueGroupingComparator(GroupComparator.class);
		
		conf.setMapperClass(MapClass.class);        
		conf.setReducerClass(Reduce.class);

		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					i++;
					//conf.setNumMapTasks(Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else if ("-s".equals(args[i])) {
					shift = Integer.parseInt(args[++i]);
					conf.set("shift", Integer.toString(shift));
				} else if ("-p".equals(args[i])) {
					numOfPartition = Integer.parseInt(args[++i]);
					conf.set("numOfPartition", Integer.toString(numOfPartition)); 
				} else if ("-d".equals(args[i])) { 
					dimension = Integer.parseInt(args[++i]);
					conf.set("dimension", Integer.toString(dimension)); 
				} else if ("-k".equals(args[i])) {
					knn = Integer.parseInt(args[++i]);
					conf.set("knn", Integer.toString(knn));
					System.out.println("knn is : " + knn); 
				} else if ("-c".equals(args[i])) {
					clusterConfiguration = args[++i];
					conf.set("clusterconfiguration", clusterConfiguration); 
/*				} else if ("-sj".equals(args[i])) {
					self_join = Boolean.parseBoolean(args[++i]);
					conf.set("self_join", Boolean.toString(self_join)); 
*/				} else if ("-outer".equals(args[i])) {
					outer = args[++i];
					conf.set("outer", outer); 
				} else if ("-inner".equals(args[i])) {
					inner = args[++i];
					conf.set("inner", outer); 
				} else {
					other_args.add(args[i]);
				}
				conf.setNumReduceTasks(numOfPartition * shift);
				//conf.setNumReduceTasks(0);
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

		String base_dir = "/user/hadoop/" + clusterConfiguration + "/";
		DistributedCache.addCacheFile(new URI(base_dir + "RandomShiftVectors"), conf);

		for (int i = 0; i < shift; i++) {

			DistributedCache.addCacheFile(
				new URI(base_dir + "range-" + outer + "-" + inner + "-" + knn
				   + "/" + "Rrange" + Integer.toString(i)), conf);
			DistributedCache.addCacheFile(
				new URI(base_dir + "range-" + outer + "-" + inner + "-" + knn
				   + "/" + "Srange" + Integer.toString(i)), conf);

		}
		//System.out.printf("shift %d partsize %d\n", shift, numOfPartition);
		
		JobClient.runJob(conf);
		return 0;
	}
  
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BPhase2(), args);
		System.exit(res);
	}
}
