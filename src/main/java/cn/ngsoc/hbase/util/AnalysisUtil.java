package cn.ngsoc.hbase.util;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据解析辅助类 Created by ${薛龙虎} on 15/3/13.
 */
public class AnalysisUtil {
	/**
	 * 将一行数据解析为一个个字段
	 * <p/>
	 * 示例数据 ,'ab\'c',bcd,,'ce\',d','ce,c','bcd',
	 *
	 * @param str
	 *            要解析的字符串
	 * @param delim
	 *            间隔符,示例中的间隔符为,
	 * @param identi
	 *            字段包含符号，如‘，“等等. 示例中字段包含符号即为 ’
	 * @return 返回List<String>
	 */
	public static List<String> StringToColumns(String str, char delim, char identi) {

		List<String> colList = new ArrayList<String>();

		char[] chars = str.toCharArray();

		int temp = 0;

		StringBuffer strVal = new StringBuffer();

		for (int i = 0; i < chars.length; i++) {

			// 如果为标示符，做一个记录
			if (chars[i] == identi) {

				if (i == 0) {
					temp = 1;
					continue;
				}

				// 字段第一次出现
				if (temp == 0) {
					temp = 1;
					continue;
				}

				if (temp == 1 && chars[i - 1] == '\\') {
					strVal.append(chars[i]);
					continue;
				}
				temp = 0;
				/*
				 * colList.add(strVal.toString()); strVal = new StringBuffer();
				 */
				continue;

			}

			if (chars[i] == delim) {
				if (temp == 0) {
					colList.add(strVal.toString());
					strVal = new StringBuffer();
					continue;
				}

				if (temp == 1) {
					strVal.append(chars[i]);
					continue;
				}
			}
			strVal.append(chars[i]);
		}

		colList.add(strVal.toString());

		return colList;
	}

	/**
	 * 将一行数据解析为一个Map(key:字段名,value:字段值)
	 * <p/>
	 * 示例数据 ,'ab\'c',bcd,,'ce\',d','ce,c','bcd',
	 *
	 * @param str
	 *            要解析的字符串
	 * @param colNameList
	 *            字段名列表
	 * @param delim
	 *            间隔符,示例中的间隔符为,
	 * @param identi
	 *            字段包含符号，如‘，“等等. 示例中字段包含符号即为 ’
	 * @return 返回Map<String, String>
	 */
	public static Map<String, String> StringToMap(String str, List<String> colNameList, char delim, char identi) {
		final Map<String, String> colMap = new HashMap<String, String>();

		List<String> colList = AnalysisUtil.StringToColumns(str, delim, identi);
		// 异常数据,直接返回
		if (colNameList.size() != colList.size()) {
			return null;
		}

		for (int i = 0; i < colNameList.size(); i++) {
			colMap.put(colNameList.get(i), colList.get(i));
		}

		return colMap;
	}

	public static List<String> dealString(String line, char separator, char escape) {
		List<String> result = new ArrayList<String>();
		boolean flag = false;
		char ch = ' ';
		StringBuffer valule = new StringBuffer();
		for (int i = 0; i < line.length(); i++) {
			ch = line.charAt(i);
			if (ch == escape) {
				if (!flag) {
					flag = !flag;
				} else if (line.charAt(Math.max(i - 1, 0)) == '\\') {
					valule.append(ch);
				} else {
					flag = !flag;
				}
				continue;
			} else if (ch == separator) {
				if (flag) {
					valule.append(ch);
				} else {
					result.add(valule.toString());
					valule.delete(0, valule.length());
				}
				continue;
			}
			valule.append(ch);
		}

		result.add(valule.toString());
		return result;
	}

	/**
	 * 利用md5加密字符串
	 *
	 * @param str
	 *            需要加密的字符串
	 * @return 返回加密过的字符串
	 */
	public static String md5(String str) {
		String retStr = "";

		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(str.getBytes());
			retStr = new BigInteger(1, md.digest()).toString(16);

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		return retStr;
	}
}
