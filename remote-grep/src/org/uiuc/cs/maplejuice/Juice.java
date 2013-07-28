package org.uiuc.cs.maplejuice;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Word count Juice task example.  This is only an example; other ways
 * of implementing this are possible, but you must keep all the distributed
 * functionality separate from Maple/Juice tasks.
 *
 * NOTE: assumes input is already sorted by key!
 *
 * Usage: juice <input file> <output file>
 */
public class Juice {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("usage: java -jar Juice.jar <input file> <output file>");
			return;
		}

		String inputFilePath = args[0];
		String outputFilePath = args[1];

		File file = new File(outputFilePath);
		FileWriter fw;

		try {
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			// We're going to write to this file
			fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);

			// Open up file to read in words
			Scanner fileScanner = null;
			try {
				fileScanner = new Scanner(new File(inputFilePath));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			// Map to store juice output
			HashMap<String, Integer> juiceOutputMap = new HashMap<String, Integer>();

			// Loop through each line
			while (fileScanner.hasNextLine()) {
				// In each line, add each key:value to our map
				String[] keyValuePair = fileScanner.nextLine().split(":");
				String key = keyValuePair[0];
				// Create key if needed
				int count = juiceOutputMap.containsKey(key) ? juiceOutputMap
						.get(key) : 0;
				juiceOutputMap.put(key, count + 1);
			}

			// Write map to output file
			for (String key : juiceOutputMap.keySet()) {
				bw.write(key + ":" + juiceOutputMap.get(key));
				bw.newLine();
			}
			fileScanner.close();
			bw.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
}
