package com.tan.dkp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CoefMapper extends Mapper<LongWritable, Text, Text, Text> {
	public Text mapKey = new Text();
	public Text mapVal = new Text();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] lineSp = value.toString().trim()
				.split(Tag.ITEMSEPARATOR.toString());
		String reg = Tag.INNERSEPARATOR.toString();
		String[] codeSp = lineSp[0].split(reg);
		String identifier = codeSp[3];// 应届历届标记
		String collegeCode = codeSp[5];
		String majorCode = codeSp[6];
		mapKey.set(collegeCode);
		for (int i = 1; i < lineSp.length; i++) {
			String[] scoSp = lineSp[i].split(reg);
			String sid = scoSp[0];// 成绩类别
			if (sid.equals(Tag.LA.toString())) {
				Double all = 0.0;
				Integer num = 0;
				for (int j = 2; j < scoSp.length; j += 2) {
					all += Double.parseDouble(scoSp[j]);
					num++;
				}
				mapVal.set(majorCode + Tag.INNERSEPARATOR + identifier
						+ Tag.INNERSEPARATOR + Tag.LA + Tag.INNERSEPARATOR
						+ all + Tag.INNERSEPARATOR + num);
				context.write(mapKey, mapVal);
			} else if (sid.equals(Tag.LB.toString())) {
				Double all = 0.0;
				Integer num = 0;
				for (int j = 2; j < scoSp.length; j += 2) {
					all += Double.parseDouble(scoSp[j]);
					num++;
				}
				mapVal.set(majorCode + Tag.INNERSEPARATOR + identifier
						+ Tag.INNERSEPARATOR + Tag.LB + Tag.INNERSEPARATOR
						+ all + Tag.INNERSEPARATOR + num);
				context.write(mapKey, mapVal);
			} else if (sid.equals(Tag.LC.toString())) {
				mapVal.set(majorCode + Tag.INNERSEPARATOR + identifier
						+ Tag.INNERSEPARATOR + Tag.LC + Tag.INNERSEPARATOR
						+ scoSp[2] + Tag.INNERSEPARATOR + "1");
				context.write(mapKey, mapVal);
			} else if (sid.equals(Tag.LD.toString())) {
				mapVal.set(majorCode + Tag.INNERSEPARATOR + identifier
						+ Tag.INNERSEPARATOR + Tag.LD + Tag.INNERSEPARATOR
						+ scoSp[2] + Tag.INNERSEPARATOR + "1");
				context.write(mapKey, mapVal);
			}
		}
	}
}