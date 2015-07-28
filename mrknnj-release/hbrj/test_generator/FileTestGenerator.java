import java.util.*;
import java.lang.*;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FileTestGenerator {
	
	public static void main(String[] args) {

		String file_output_path = "/Users/aleksandrashanina/mapreduce/mrknnj-release/hbrj/input2/50000000_inner.txt"; 
		int million = 1000000;
		int billion = 1000*million;
		int lineBlock = million;
		int totalLineCount = 50; // multiple of lineBlock of lines to write to a file
		int idCount = 0; 
		String filler = "Siskiyou,?CA";
		StringBuilder sb;
		int upperBound = 1000000000; // last 4 0s are for dp.
		BufferedWriter bw;

		Random r = new Random();
		Random r2 = new Random();

		// Creating a file 
		try {
			File file = new File(file_output_path);
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			bw = new BufferedWriter(fw);

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			// Writing multiples of lineBlock to a file

			for (int j = 0; j < totalLineCount; j++) {
				sb = new StringBuilder();
				for (int i = 0; i < lineBlock; i ++) {
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

				bw.append(sb);
			}

			System.out.println("Done");

			
			bw.close();
			
	 
		} catch (IOException e) {
			e.printStackTrace();
		}

		
			


	} // end main
}
