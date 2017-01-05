package cn.ngsoc.hbase.util;

/**
 * Created by zhenjie.wang on 2015/8/13.
 */
public class BackCostUtil {

    public static String backCost(String cost){
        if(cost == null) return "";
        if(cost.startsWith("-")){
            String basic = cost.substring(2,cost.length());
            String index = cost.substring(1,2);
            if(basic==null || index ==null)return cost;
            Integer int_basic = Integer.valueOf(basic);
            if(int_basic==0) return 0+"";
            Integer int_index = 10;
            for(int i = 1; i< Integer.valueOf(index); i++){
                int_index*=10;
            }
            String result = String.valueOf(fix((double)int_basic/int_index, Integer.valueOf(index)));
            if(result.endsWith("0")){
                result = result.substring(0,result.length()-1);
            }
            if(result.length()<5)return result+"0";
            return result;
        }else{
            return  cost;
        }
    }

    private static  double fix(double val,int fix){
        if (val==0) return val;
        int p = (int) Math.pow(10,fix);
        return (double)((int)(val*p))/p;
    }

    public static void main(String[] args){
      System.out.print(backCost("-311101"));
    }
}
