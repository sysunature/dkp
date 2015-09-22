package com.tan.dkp;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

/**
 * 项目
 *
 */
public class Driver extends Configured implements Tool {
	public final static String HdfsUri = "hdfs://master:9000";
	public final static String VirtualCoefPath = "/tmp/intervals";

	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err
					.println("Usage: Driver <inputFile> <dataware> <coefficient> <result>");
			System.exit(2);
		}
		PropertyConfigurator.configure("log4j.properties");
		Configuration conf = new Configuration();
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		FileSystem fs = FileSystem.get(URI.create(HdfsUri), conf);
		Path path = new Path(Driver.VirtualCoefPath);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
				fs.create(path)));
		bw.write(args[2]);
		bw.close();
		int ret = ToolRunner.run(new Driver(), args);
		System.exit(ret);

	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("mapreduce.output.textoutputformat.separator",
				Tag.INNERSEPARATOR.toString());
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		Job job = Job.getInstance(conf,
				"Fisrt Job Integrate all ems-centered information");
		job.setJarByClass(Driver.class);
		job.setMapperClass(PrevMapper.class);
		job.setReducerClass(PrevReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

		Job job2 = Job.getInstance(conf,
				"Second Job Calculate all business sector coefficients");
		job2.setJarByClass(Driver.class);
		job2.setMapperClass(CoefMapper.class);
		job2.setReducerClass(CoefReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.waitForCompletion(true);

		Job job3 = Job.getInstance(conf,
				"Third Job Calculate ems-centered final score");
		job3.setJarByClass(Driver.class);
		job3.setMapperClass(CalcMapper.class);
		job3.setReducerClass(CalcReducer.class);
		job3.setPartitionerClass(CalcPartitioner.class);
		job3.setGroupingComparatorClass(CalcGrouping.class);
		job3.setSortComparatorClass(CodeComparator.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(NullWritable.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job3, new Path(args[1]));
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));

		boolean success = job3.waitForCompletion(true);
		return success ? 0 : 1;
	}
}
