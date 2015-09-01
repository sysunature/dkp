package com.tan.dkp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CoefReducer extends Reducer<Text, Text, Text, Text> {
	public Text reduKey = new Text();
	public Text reduVal = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		HashMap<String, String> busCoef = new HashMap<String, String>();
		HashMap<String, String> busItem = new HashMap<String, String>();
		String majorKey = null;
		String majorCoefVal = null;
		HashMap<String, String> collegeCoef = new HashMap<String, String>();
		HashMap<String, String> collegeItem = new HashMap<String, String>();
		String collegeKey = null;
		String collegeCoefVal = null;
		Integer majorItemVal = null;
		Integer collegeItemVal = null;
		String reg = Tag.INNERSEPARATOR.toString();
		for (Text val : values) {
			String[] scoSp = val.toString().split(reg);
			majorKey = scoSp[0] + Tag.INNERSEPARATOR + scoSp[1]
					+ Tag.INNERSEPARATOR + scoSp[2];
			majorCoefVal = busCoef.get(majorKey);
			if (majorCoefVal == null) {
				busCoef.put(majorKey, scoSp[3]);
				busItem.put(majorKey, scoSp[4]);
			} else {
				majorCoefVal = ""
						+ (Double.parseDouble(majorCoefVal) + Double
								.parseDouble(scoSp[3]));
				majorItemVal = Integer.parseInt(busItem.get(majorKey))
						+ Integer.parseInt(scoSp[4]);
				busCoef.put(majorKey, majorCoefVal);
				busItem.put(majorKey, "" + majorItemVal);
			}
			collegeKey = key.toString() + Tag.INNERSEPARATOR + scoSp[1]
					+ Tag.INNERSEPARATOR + scoSp[2];
			collegeCoefVal = collegeCoef.get(collegeKey);
			if (collegeCoefVal == null) {
				collegeCoef.put(collegeKey, scoSp[3]);
				collegeItem.put(collegeKey, scoSp[4]);
			} else {
				collegeCoefVal = ""
						+ (Double.parseDouble(collegeCoefVal) + Double
								.parseDouble(scoSp[3]));
				collegeItemVal = Integer.parseInt(collegeItem.get(collegeKey))
						+ Integer.parseInt(scoSp[4]);
				collegeCoef.put(collegeKey, collegeCoefVal);
				collegeItem.put(collegeKey, "" + collegeItemVal);
			}
		}
		Iterator<Entry<String, String>> iter = busCoef.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, String> entry = iter.next();
			majorKey = entry.getKey();
			majorCoefVal = entry.getValue();
			Integer busNum = Integer.parseInt(busItem.get(majorKey));
			String org = majorKey.substring(0, 3);
			collegeKey = new StringBuffer(majorKey).delete(3, 6).toString();
			collegeCoefVal = collegeCoef.get(collegeKey);
			Integer orgNum = Integer.parseInt(collegeItem.get(collegeKey));
			Double coef = (Double.parseDouble(majorCoefVal) / busNum)
					/ (Double.parseDouble(collegeCoefVal) / orgNum);
			reduKey.set(org + Tag.INNERSEPARATOR + majorKey);
			reduVal.set("" + coef);
			context.write(reduKey, reduVal);
		}

	}
}
