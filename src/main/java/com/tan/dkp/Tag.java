package com.tan.dkp;

public enum Tag {
	LA("A"), LB("B"), LC("C"), LD("D"), LE("E"), INNERSEPARATOR("\t"), // 对应每个人的每行汇总数据中，同一类别元素之间的分隔符
	ITEMSEPARATOR("#"), // 对应每个人的每行汇总数据中，不同类别之间的分隔符,禁止使用特殊字符
	CELLSEPARATOR(",");// 数据文件元素分隔符
	private String tname = null;

	private Tag(String _tname) {
		this.tname = _tname;
	}

	@Override
	public String toString() {
		return tname;
	}
}
