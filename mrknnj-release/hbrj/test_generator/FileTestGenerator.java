import java.util.*;
import java.lang.*;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FileTestGenerator {
	
	public static void main(String[] args) {

		int million = 1000000;
		int billion = 1000*million;
		int lineCount = 50*million; // 50 million
		int idCount = 0; 
		String filler = "Siskiyou,?CA";
		StringBuilder sb = new StringBuilder();
		int upperBound = 1000000000; // last 4 0s are for dp.
		String output_file = "/Users/aleksandrashanina/mapreduce/mrknnj-release/hbrj/input2/50000000_inner.txt";

		Random r = new Random();
		Random r2 = new Random();

		for (int i = 0; i < lineCount; i ++) {
			sb.append(idCount); //id
			sb.append(" ");
			int rand = r.nextInt(upperBound);
			double fixed = (double) rand / 10000; 
			//System.out.println(fixed);
			sb.append(fixed); // x coord
			sb.append(" ");
			int rand2 = r2.nextInt(upperBound);
			double fixed2 = (double) rand2 / 10000; 
			//System.out.println(fixed2);
			sb.append(fixed2); // x coord
			sb.append(" ");
			sb.append(filler);
			sb.append("\n");
			idCount++;
		}
		
		System.out.println("StringBuilder done");

		try {
 
			String content = "This is the content to write into file";
 
			File file = new File(output_file);
 
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
 
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.append(sb);
			bw.close();
 
			System.out.println("Done");
 
		} catch (IOException e) {
			e.printStackTrace();
		}



	}
}
