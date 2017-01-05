package cn.ngsoc.hbase.util;

import java.util.List;

public class IListUtil {

	public static String implode(List<String> strList) {
		String res = "";
		if (strList == null || strList.size() == 0) {
			return res;
		}
		for (String str : strList) {
			res += str + ",";
		}
		return res.substring(0, res.length() - 1);
	}
}
