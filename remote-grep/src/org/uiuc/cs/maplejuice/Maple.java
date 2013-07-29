package org.uiuc.cs.maplejuice;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

/**
 * Word count Maple task example. This is only an example; other ways of
 * implementing this are possible, but you must keep all the distributed
 * functionality separate from Maple/Juice tasks.
 * 
 * Usage: maple <input file> <output file>
 */
public class Maple {

	/**
	 * @param args
	 *            Input and output file
	 */
	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("usage: java -jar Maple.jar <input file> <output file>");
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

			// Loop through each line
			while (fileScanner.hasNextLine()) {
				// In each line, loop through each word. Scanner breaks on \s by
				// default
				Scanner lineScanner = new Scanner(fileScanner.nextLine());
				while (lineScanner.hasNext()) {
					// Add the key to the output file
					String word = lineScanner.next();
					word = word.trim().replaceAll("\\W", "");
					bw.write(word + ":" + 1);
					bw.newLine();
				}
				lineScanner.close();
			}
			fileScanner.close();
			bw.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
}
