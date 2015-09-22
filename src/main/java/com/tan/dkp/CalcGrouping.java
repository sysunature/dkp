package com.tan.dkp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CalcGrouping extends WritableComparator {
	public final static String reg = Tag.INNERSEPARATOR.toString();

	public CalcGrouping() {
		super(Text.class, true);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable a, WritableComparable b) {
		Text ta = (Text) a;
		Text tb = (Text) b;
		String[] taSp = ta.toString().split(reg);
		String[] tbSp = tb.toString().split(reg);
		return (taSp[6] + Tag.INNERSEPARATOR + taSp[3]).compareTo(tbSp[6]
				+ Tag.INNERSEPARATOR + tbSp[3]);
	}
}
