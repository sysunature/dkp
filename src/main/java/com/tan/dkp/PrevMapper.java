package com.tan.dkp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PrevMapper extends Mapper<LongWritable, Text, Text, Text> {
	public Text mapKey = new Text();
	public Text mapValue = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String filename = ((FileSplit) context.getInputSplit()).getPath()
				.getName();
		String reg = Tag.CELLSEPARATOR.toString();
		if (filename.equals("la.csv")) {
			String[] lineSp = value.toString().trim().split(reg);
			String quarter = lineSp[0];
			String sno = lineSp[1];
			String score = lineSp[2];
			mapKey.set(sno);
			mapValue.set("" + Tag.LA + Tag.INNERSEPARATOR + quarter
					+ Tag.INNERSEPARATOR + score);
			context.write(mapKey, mapValue);
		} else if (filename.equals("lb.csv")) {
			String[] lineSp = value.toString().trim().split(reg);
			String quarter = lineSp[0];
			String sno = lineSp[1];
			String score = lineSp[2];
			mapKey.set(sno);
			mapValue.set("" + Tag.LB + Tag.INNERSEPARATOR + quarter
					+ Tag.INNERSEPARATOR + score);
			context.write(mapKey, mapValue);
		} else if (filename.equals("lc.csv")) {
			String[] lineSp = value.toString().trim().split(reg);
			String year = lineSp[0];
			String retst = lineSp[1];// 补考次数
			String skind = lineSp[2];// 考试类别
			String sno = lineSp[3];
			String score = lineSp[4];
			mapKey.set(sno);
			mapValue.set("" + Tag.LC + Tag.INNERSEPARATOR + year
					+ Tag.INNERSEPARATOR + skind + Tag.INNERSEPARATOR + retst
					+ Tag.INNERSEPARATOR + score);
			context.write(mapKey, mapValue);
		} else if (filename.equals("ld.csv")) {
			String[] lineSp = value.toString().trim().split(reg);
			String year = lineSp[0];
			String sno = lineSp[1];
			String score = lineSp[2];
			mapKey.set(sno);
			mapValue.set("" + Tag.LD + Tag.INNERSEPARATOR + year
					+ Tag.INNERSEPARATOR + score);
			context.write(mapKey, mapValue);
		} else if (filename.equals("le.csv")) {
			String[] lineSp = value.toString().trim().split(reg);
			String year = lineSp[0];
			String fullCode = lineSp[1];
			String collegeCode = fullCode.substring(0, 3);
			String majorCode = fullCode.substring(0, 6);
			String gradYear = fullCode.substring(fullCode.length() - 2);
			String sno = lineSp[2];
			mapKey.set(sno);
			String current = "" + (Integer.parseInt(year) - 1);
			String identifier = gradYear.equals(current.substring(current
					.length() - 2)) ? "Y" : "N";
			mapValue.set("" + Tag.LE + Tag.INNERSEPARATOR + year
					+ Tag.INNERSEPARATOR + gradYear + Tag.INNERSEPARATOR
					+ identifier + Tag.INNERSEPARATOR + fullCode
					+ Tag.INNERSEPARATOR + collegeCode + Tag.INNERSEPARATOR
					+ majorCode);
			context.write(mapKey, mapValue);
		}
	}
}
