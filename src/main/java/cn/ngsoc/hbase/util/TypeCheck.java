package cn.ngsoc.hbase.util;

import java.util.List;

/**
 * Created by zhenjie.wang on 2015/8/13.
 */
public class TypeCheck {

	private enum LongtypeCheck {
		// sessionid,sqlid,dip,replyaccessid,smac,dmac;
		accessid, replyaccessid, c4;
	}

	private enum InttypeCheck {
		// policyid,effectrow,shgid,apptypeid,srcid,datafrom,policyalertgrade;
		effectrow, shgid, apptypeid, srcid, datafrom, policyalertgrade;// ,poid;
	}

	private enum FloattypeCheck {
		code, level, cost, resultflag;
	}

	public static boolean checkLong(String source) {
		for (LongtypeCheck l : LongtypeCheck.values()) {
			if (l.toString().equals(source.toLowerCase())) {
				return true;
			}
		}
		return false;
	}

	public static boolean checkInt(String source) {
		for (InttypeCheck i : InttypeCheck.values()) {
			if (i.toString().equals(source.toLowerCase())) {
				return true;
			}
		}
		return false;
	}

	public static boolean checkFloat(String source) {
		for (FloattypeCheck f : FloattypeCheck.values()) {
			if (f.toString().equals(source.toLowerCase())) {
				return true;
			}
		}
		return false;
	}

	public static boolean checkString(String source, List<String> strList) {
		if (strList.contains(source)) {
			return true;
		}
		return false;
	}
}
