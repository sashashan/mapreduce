// filename: ExternalSort.java
package de.lmu.ifi.dbs.elki.index;
import java.lang.Runtime;
import java.lang.Process;
import java.util.*;
import java.io.*;

/**
* Goal: offer a generic external-memory sorting program in Java.
* 
* It must be : 
*  - hackable (easy to adapt)
*  - scalable to large files
*  - sensibly efficient.
*
* This software is in the public domain.
*
* By Daniel Lemire, April 2010
* http://www.daniel-lemire.com/
*
* Modified version based on the original version. CZ & JEFF
*
*/
public class ExternalSort {
	public static final int bufferSize = 8 * 1024 * 1024;
	public static final double mb = 1024d * 1024d;
	public static final long mbl = 1024 * 1024;

	/**
	 * This will simply load the file by blocks of x rows, then
	 * sort them in-memory, and write the result to a bunch of 
	 * temporary files that have to be merged later.
	 * 
	 * @param file some flat  file
	 * @param blockSize the size of a block in megabytes
	 * @return a list of temporary flat files
	 */
	public static List<File> sortInBatch(File file, Comparator<String> cmp, 
		long blockSize) throws IOException {

		Runtime r = Runtime.getRuntime();
		blockSize = blockSize * mbl;
		long viableBlockSize = blockSize;

		long startHeapSize = r.totalMemory() - r.freeMemory();
		if( r.maxMemory() - startHeapSize < blockSize ) 
			viableBlockSize = r.maxMemory() - startHeapSize - ( 100 * mbl );

		long heapSize = startHeapSize;
		System.out.println( "heap at sib entry: " + ( heapSize / mb ) );

		List<File> files = new LinkedList<File>();
		//long total = 0;
		int filecounter = 0;
		BufferedReader fbr = new BufferedReader(new FileReader(file), 
				bufferSize);

		try{
			List<String> tmplist =  new LinkedList<String>();
			String line = "";
			try {
				while((line = fbr.readLine() ) != null) {
					//Calculate the real minimum memory usage of a String
					//total += 8 * (int) (((line.length() * 2) + 45) / 8) + 8;
					tmplist.add(line);

					heapSize = r.totalMemory() - r.freeMemory();
					if ( heapSize - startHeapSize >= viableBlockSize ) {
						System.out.println( "strings: " + tmplist.size() );

						heapSize = r.totalMemory() - r.freeMemory();
						System.out.println( "heap before sortAndSave: " + 
								( heapSize / mb ) );

						files.add(sortAndSave(tmplist,cmp, filecounter));

						heapSize = r.totalMemory() - r.freeMemory();
						System.out.println( "heap after sortAndSave: " + 
								( heapSize / mb ) );

						tmplist.clear();
						r.gc();

						heapSize = r.totalMemory() - r.freeMemory();
						startHeapSize = heapSize;

						if( r.maxMemory() - startHeapSize < blockSize ) 
							viableBlockSize = r.maxMemory() - startHeapSize 
								- ( 100 * mbl );
						else 
							viableBlockSize = blockSize;

						System.out.println( "heap after gc: " + 
								( heapSize / mb ) );

						System.out.printf("file# %d\n", filecounter++);
						//total = 0;
					}

				}

				if (tmplist.size() > 0) {

					heapSize = r.totalMemory() - r.freeMemory();
					System.out.println( "heap before sortAndSave: " + 
							( heapSize / mb ) );

					files.add(sortAndSave(tmplist,cmp, filecounter));

					heapSize = r.totalMemory() - r.freeMemory();
					System.out.println( "heap after sortAndSave: " + 
							( heapSize / mb ) );

					tmplist.clear();
					r.gc();

					heapSize = r.totalMemory() - r.freeMemory();
					System.out.println( "heap after gc: " + ( heapSize / mb ) );

					System.out.printf("file# %d\n", filecounter++);
	
				}

			} catch(EOFException oef) {

				System.out.println("end of file reached!!!");
				System.exit(-1);
				if(tmplist.size()>0) {
					files.add(sortAndSave(tmplist,cmp, 100));
					tmplist.clear();
				}

			}
		} finally {
			fbr.close();
		}

		r.gc();

		heapSize = r.totalMemory() - r.freeMemory();
		System.out.println( "heap at sib end: " + ( heapSize / mb ) );
	
		return files;
	}


	public static File sortAndSave(List<String> tmplist, Comparator<String> cmp,
			int filecounter) throws IOException  {
		Collections.sort(tmplist,cmp);  // 
		
		//File newtmpfile = File.createTempFile("sortInBatch", "flatfile");
		//newtmpfile.deleteOnExit();

		File newtmpfile = 
			new File("sortandsave" + Integer.toString(filecounter));

		BufferedWriter fbw = 
			new BufferedWriter(new FileWriter(newtmpfile), bufferSize);

		try {
			for(String r : tmplist) {
				fbw.write(r);
				fbw.newLine();
			}
		} finally {
			fbw.close();
		}
		return newtmpfile;
	}
	/**
	 * This merges a bunch of temporary flat files 
	 * @param files
	 * @param output file
         * @return The number of lines sorted. (P. Beaudoin)
	 */
	public static File mergeSortedFiles(List<File> files, File outputfile, 
			Comparator<String> cmp) throws IOException {

		PriorityQueue<BinaryFileBuffer> pq = 
			new PriorityQueue<BinaryFileBuffer>();

		Runtime r = Runtime.getRuntime();
		long heapSize = r.totalMemory() - r.freeMemory();
		System.out.println( "heap at mergesf entry: " + ( heapSize / mb ) );
	
		for (File f : files) {
			BinaryFileBuffer bfb = new BinaryFileBuffer(f,cmp);
			pq.add(bfb);
		}

		heapSize = r.totalMemory() - r.freeMemory();
		System.out.println( "heap after files open: " + ( heapSize / mb ) );
	

		File newtmpfile = File.createTempFile("mergesort", 
				"sortedfile", new File(parent));

		//newtmpfile.deleteOnExit();  // take effect after the program ends
		BufferedWriter fbw = new BufferedWriter(new FileWriter(newtmpfile),
				bufferSize);

		int rowcounter = 0;
		try {
			while(pq.size()>0) {
				BinaryFileBuffer bfb = pq.poll();
				String r1 = bfb.pop();
				fbw.write(r1);
				fbw.newLine();
				++rowcounter;
				if(bfb.empty()) {
					bfb.fbr.close();
					bfb.originalfile.delete();// we don't need you anymore
					r.gc();
				} else {
					pq.add(bfb); // add it back
				}
			}
		} finally {
			fbw.close();
		}

		r.gc();

		heapSize = r.totalMemory() - r.freeMemory();
		System.out.println( "heap at mergesf exit: " + ( heapSize / mb ) );
	
		return newtmpfile;
	}

	public static void externalSort( String inputfile, String outputfile, 
		int size ) throws IOException{

		Comparator<String> comparator = new Comparator<String>() {
			public int compare(String r1, String r2)
			{
				//return r1.compareTo(r2);
				//Make sure fields in the string is seperated by " "
				//and Zvalues is the first field and have the same width
				String[] parts1 = r1.split(" ");
				String[] parts2 = r2.split(" ");
				return parts1[0].compareTo(parts2[0]);
			}
		};

		// Initial sort pass zero
		File inf = new File(inputfile);
		parent = inf.getAbsoluteFile().getParent();

		List<File> l = sortInBatch(inf, comparator, size) ;

		System.out.println("Pass Zero is finished");


		// Multiple pass 1 .. N merge sort
		int fanout = 10;
		int pc = 0;
		List<File> passResult = new LinkedList<File>();
		while(true) {
			while(l.size() > 0) {

				List<File> pass = new LinkedList<File>();
				for (int i = 0; l.size() > 0 && i < fanout; i++)
					pass.add(l.remove(0));
				File f = mergeSortedFiles(pass, new File(outputfile), 
															comparator);
				passResult.add(f);

			}  // at end we complete one pass

			pc++;
			System.out.printf("Pass %d is done ~sasha 3 SHOW ME ~\n", pc);

			if( l.size() == 0 && passResult.size() == 1 ) {

				boolean flag = 
					//passResult.get(0).renameTo(new File(parent, outputfile));
					passResult.get(0).renameTo(new File(outputfile));

				if (!flag) {
					System.out.println("base dir : " + parent);
					System.out.println("file can not be rename!\n");
				}
				break;

			}

			l = passResult;
			passResult = new LinkedList<File>();

		} // while

		System.out.println("Pass N is finished");
	}

	public static String parent = "";

	public static void main(String[] args) throws IOException {
		if(args.length<3) {
			System.out.println("Usage: <input> <output> <blockSize(mb)>");
			return;
		}

		String inputfile = args[0];
		String outputfile = args[1];
		int size = Integer.valueOf(args[2]);

		externalSort( inputfile, outputfile, size );

	}
}


class BinaryFileBuffer  implements Comparable<BinaryFileBuffer>{
	public static int BUFFERSIZE = 512;
	public BufferedReader fbr;
	private List<String> buf = new LinkedList<String>();
	int currentpointer = 0;
	Comparator<String> mCMP;
	public File originalfile;
	
	public BinaryFileBuffer(File f, Comparator<String> cmp) throws IOException {
		originalfile = f;
		mCMP = cmp;
		fbr = new BufferedReader(new FileReader(f), ExternalSort.bufferSize);
		reload();
	}
	
	public boolean empty() {
		return buf.size()==0;
	}
	
	private void reload() throws IOException {
		  buf.clear();
		  try {
		  	  String line;
	 		  while((buf.size()<BUFFERSIZE) && 
					  ((line = fbr.readLine()) != null))
				buf.add(line);
			} catch(EOFException oef) {
			}		
	}
	
	
	public String peek() {
		if(empty()) return null;
		return buf.get(currentpointer);
	}
	public String pop() throws IOException {
	  String answer = peek();
	  ++currentpointer;
	  if(currentpointer == buf.size()) {
		  reload();
		  currentpointer = 0;
	  }
	  return answer;
	}
	
	public int compareTo(BinaryFileBuffer b) {
		return mCMP.compare(peek(), b.peek());
	}

}
