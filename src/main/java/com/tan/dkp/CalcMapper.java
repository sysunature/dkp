package com.tan.dkp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CalcMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	public HashMap<String, String> coef = new HashMap<String, String>();
	public String hashKey = null;
	public String hashVal = null;
	public Text mapKey = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] lineSp = value.toString().trim()
				.split(Tag.ITEMSEPARATOR.toString());
		String reg = Tag.INNERSEPARATOR.toString();
		String[] codeSp = lineSp[0].split(reg);
		String header = codeSp[6] + Tag.INNERSEPARATOR + codeSp[3];
		Double la = 0.0;
		Double lb = 0.0;
		Double lc = 0.0;
		Double ld = 0.0;
		Double tzxs = 0.0;
		Double actual = 0.0;
		for (int i = 1; i < lineSp.length; i++) {
			String[] scoSp = lineSp[i].split(reg);
			Double sum = 0.0;
			Integer items = 0;
			String id = scoSp[0];
			if (id.equals(Tag.LA.toString())) {
				for (int j = 2; j < scoSp.length; j += 2) {
					sum += Double.parseDouble(scoSp[j]);
					items++;
				}

				la = sum / items;
				String strtzxs = coef.get(header + Tag.INNERSEPARATOR + id);
				if (strtzxs == null)
					continue;
				tzxs = Double.parseDouble(strtzxs);
				la /= tzxs;
				sum = 0.0;
				items = 0;
			} else if (id.equals(Tag.LB.toString())) {
				for (int j = 2; j < scoSp.length; j += 2) {
					sum += Double.parseDouble(scoSp[j]);
					items++;
				}
				lb = sum / items;
				String strtzxs = coef.get(header + Tag.INNERSEPARATOR + id);
				if (strtzxs == null)
					continue;
				tzxs = Double.parseDouble(strtzxs);
				lb /= tzxs;
				sum = 0.0;
				items = 0;
			} else if (id.equals(Tag.LC.toString())) {
				lc = Double.parseDouble(scoSp[2]);
				String strtzxs = coef.get(header + Tag.INNERSEPARATOR + id);
				if (strtzxs == null)
					continue;
				tzxs = Double.parseDouble(strtzxs);
				lc /= tzxs;
			} else if (id.equals(Tag.LD.toString())) {
				ld = Double.parseDouble(scoSp[2]);
				String strtzxs = coef.get(header + Tag.INNERSEPARATOR + id);
				if (strtzxs == null)
					continue;
				tzxs = Double.parseDouble(strtzxs);
				ld /= tzxs;
			}
		}
		if (codeSp[3].equals("Y")) {
			actual = la * 0.4 + lb * 0.4 + lc * 0.2;
		} else {
			actual = la * 0.6 + ld * 0.4;
		}
		mapKey.set(lineSp[0] + Tag.INNERSEPARATOR + actual);
		context.write(mapKey, NullWritable.get());
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		FileSystem hdfs = FileSystem.get(URI.create(Driver.HdfsUri),
				context.getConfiguration());
		FileStatus[] fStat = hdfs.listStatus(new Path(new BufferedReader(
				new InputStreamReader(hdfs
						.open(new Path(Driver.VirtualCoefPath)))).readLine()));
		hdfs.close();
		Path[] listPath = FileUtil.stat2Paths(fStat);
		for (Path p : listPath) {
			hdfs = FileSystem.get(URI.create(Driver.HdfsUri),
					context.getConfiguration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					hdfs.open(p)));
			String line = null;
			String reg = Tag.INNERSEPARATOR.toString();
			while ((line = br.readLine()) != null) {
				String[] lineSp = line.split(reg);
				hashKey = lineSp[1] + Tag.INNERSEPARATOR + lineSp[2]
						+ Tag.INNERSEPARATOR + lineSp[3];
				hashVal = lineSp[4];
				coef.put(hashKey, hashVal);
			}
			br.close();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		FileSystem fs = FileSystem.get(URI.create(Driver.HdfsUri),
				context.getConfiguration());
		Path path = new Path(Driver.VirtualCoefPath);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
		fs.close();
	}

}
