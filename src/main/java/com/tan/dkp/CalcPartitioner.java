package com.tan.dkp;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CalcPartitioner extends Partitioner<Text, NullWritable> {

	@Override
	public int getPartition(Text key, NullWritable value, int numPartitions) {
		String reg = Tag.INNERSEPARATOR.toString();
		String[] lineSp = key.toString().split(reg);
		String valid = lineSp[6] + Tag.INNERSEPARATOR + lineSp[3];
		return (valid.hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
