package cn.ngsoc.hbase.util;

import java.util.List;
import java.util.Set;

public class EqualValue {

	public static boolean equalLists(List<String> list1, List<String> list2) {
		if (list1 == null || list2 == null) {
			return false;
		}else if(list1.size() != list2.size()){
			return false;
		}else if(list1.containsAll(list2) && list2.containsAll(list1)){
			return true;
		}
		return false;
	}
	
	public static boolean equalListAndSet(Set<String> set1, List<String> list2) {
		if (set1 == null || list2 == null) {
			return false;
		}else if(set1.size() != list2.size()){
			return false;
		}else if(set1.containsAll(list2) && list2.containsAll(set1)){
			return true;
		}
		return false;
	}
}
