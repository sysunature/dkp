package com.tan.dkp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CodeComparator extends WritableComparator {

	protected CodeComparator() {
		super(Text.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Text ka = (Text) a;
		Text kb = (Text) b;
		String[] kasp = ka.toString().split(Tag.INNERSEPARATOR.toString());
		String[] kbsp = kb.toString().split(Tag.INNERSEPARATOR.toString());
		Integer aOrg = Integer.parseInt(kasp[5]);// 院系编码
		Integer bOrg = Integer.parseInt(kbsp[5]);
		Integer aSec = Integer.parseInt(kasp[6]);// 专业编码
		Integer bSec = Integer.parseInt(kbsp[6]);
		Double aSco = Double.parseDouble(kasp[7]);// 综合成绩
		Double bSco = Double.parseDouble(kbsp[7]);
		if (aOrg < bOrg) {
			return -1;
		} else if (aOrg > bOrg) {
			return 1;
		} else {
			if (aSec < bSec) {
				return -1;
			} else if (aSec > bSec) {
				return 1;
			} else {
				if (aSco < bSco) {
					return 1;
				} else if (aSco > bSco) {
					return -1;
				} else {
					return 0;
				}
			}
		}
	}
}
