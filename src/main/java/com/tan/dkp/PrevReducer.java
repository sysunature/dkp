package com.tan.dkp;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PrevReducer extends Reducer<Text, Text, Text, Text> {
	public Text redu = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String resuLA = null;
		String resuLB = null;
		String resuLC = null;
		String resuLD = null;
		String resuLE = null;
		String finalZ = null;// LC Part One成绩
		Integer finalZN = -1;// LC Part One补考次数
		String finalD = null;// LC Part Two成绩
		Integer finalDN = -1;// LC Part Two补考次数
		String reduVal = null;
		String reg = Tag.INNERSEPARATOR.toString();
		for (Text value : values) {
			String[] lineSp = value.toString().trim().split(reg);
			if (lineSp[0].equals(Tag.LA.toString())) {
				if (resuLA == null)
					resuLA = lineSp[0] + Tag.INNERSEPARATOR + lineSp[1]
							+ Tag.INNERSEPARATOR + lineSp[2];
				else
					resuLA = resuLA + Tag.INNERSEPARATOR + lineSp[1]
							+ Tag.INNERSEPARATOR + lineSp[2];
			} else if (lineSp[0].equals(Tag.LB.toString())) {
				if (resuLB == null)
					resuLB = lineSp[0] + Tag.INNERSEPARATOR + lineSp[1]
							+ Tag.INNERSEPARATOR + lineSp[2];
				else
					resuLB = resuLB + Tag.INNERSEPARATOR + lineSp[1]
							+ Tag.INNERSEPARATOR + lineSp[2];
			} else if (lineSp[0].equals(Tag.LC.toString())) {
				resuLC = Tag.LC.toString() + Tag.INNERSEPARATOR + lineSp[1];
				if (lineSp[2].equals("1")) {
					if (Integer.parseInt(lineSp[3]) > finalZN) {
						finalZN = Integer.parseInt(lineSp[3]);
						finalZ = lineSp[4];
					}
				} else {
					if (Integer.parseInt(lineSp[3]) > finalDN) {
						finalDN = Integer.parseInt(lineSp[3]);
						finalD = lineSp[4];
					}
				}
			} else if (lineSp[0].equals(Tag.LD.toString())) {
				resuLD = lineSp[0] + Tag.INNERSEPARATOR + lineSp[1]
						+ Tag.INNERSEPARATOR + lineSp[2];
			} else if (lineSp[0].equals(Tag.LE.toString())) {
				resuLE = lineSp[1] + Tag.INNERSEPARATOR + lineSp[2]
						+ Tag.INNERSEPARATOR + lineSp[3] + Tag.INNERSEPARATOR
						+ lineSp[4] + Tag.INNERSEPARATOR + lineSp[5]
						+ Tag.INNERSEPARATOR + lineSp[6];
			}
		}
		Double score = 0.0;
		if (finalZ != null)
			score += 0.4 * Double.parseDouble(finalZ);
		if (finalD != null)
			score += 0.6 * Double.parseDouble(finalD);
		if (score != 0.0)
			resuLC = resuLC + Tag.INNERSEPARATOR + score;
		if (resuLE == null)
			return;
		reduVal = resuLE + Tag.ITEMSEPARATOR;
		if (resuLA != null)
			reduVal = reduVal + resuLA + Tag.ITEMSEPARATOR;
		if (resuLB != null)
			reduVal = reduVal + resuLB + Tag.ITEMSEPARATOR;
		if (resuLC != null)
			reduVal = reduVal + resuLC + Tag.ITEMSEPARATOR;
		if (resuLD != null)
			reduVal = reduVal + resuLD + Tag.ITEMSEPARATOR;
		reduVal = reduVal.substring(0, reduVal.length() - 1);
		redu.set(reduVal);
		context.write(key, redu);
	}
}
