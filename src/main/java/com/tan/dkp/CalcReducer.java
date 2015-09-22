package com.tan.dkp;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalcReducer extends Reducer<Text, NullWritable, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {
		int i = 1;
		for (@SuppressWarnings("unused")
		NullWritable val : values) {
			context.write(key, new Text("" + i));
			i++;
		}
	}

}
