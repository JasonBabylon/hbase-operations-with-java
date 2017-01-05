package cn.ngsoc.hbase.util;

/**
 * Created by zhenjie.wang on 2015/8/19.
 */
public class MacOrIpconvert {

    //

    public static long ipToLong(String strIp){
        if(null==strIp || "".equals(strIp.trim())) return 0;
        long[] ip = new long[4];
        int p1 = strIp.indexOf(".");
        int p2 = strIp.indexOf(".",p1+1);
        int p3 = strIp.indexOf(".",p2+1);

        ip[0] = Long.parseLong(strIp.substring(0,p1));
        ip[1] = Long.parseLong(strIp.substring(p1+1,p2));
        ip[2] = Long.parseLong(strIp.substring(p2+1,p3));
        ip[3] = Long.parseLong(strIp.substring(p3+1));

        return (ip[0] << 24)+(ip[1] << 16)+(ip[2] << 8)+ip[3];
    }

    //

    public static String longToIp(long longIp){
        StringBuffer sb = new StringBuffer("");
        sb.append(String.valueOf((longIp >>> 24)));
        sb.append(".");
        sb.append(String.valueOf((longIp & 0x00FFFFFF) >>> 16));
        sb.append(".");
        sb.append(String.valueOf((longIp & 0x0000FFFF) >>> 8));
        sb.append(".");
        sb.append(String.valueOf((longIp & 0x000000FF)));
        return sb.toString();
    }

    //

    public static long macToString(String mac){
        if(null==mac || "".equals(mac.trim())) return 0;
        mac = mac.trim();
        mac = mac.replace("-","");
        mac = mac.replace(":","");
        return Long.parseLong(mac,16);
    }

    //

    public static String longToMac(long longMac){
        String strMac = Long.toHexString(longMac);
        while (strMac.length()<12){
            strMac = "0"+strMac;
        }
        String mac = "";
        for(int i=0;i<strMac.length(); i=i+2){
            if (mac.equals("")){
                mac = strMac.substring(i,2);
            }else{
                mac = mac + ':' +strMac.substring(i,i+2);
            }
        }
        return mac;
    }

    public static  void main(String[] args){
        System.out.println(longToMac(345052545026l));
        System.out.println(macToString("00:50:56:bc:00:02"));
        System.out.println(ipToLong("192.168.21.97"));
        System.out.println(longToIp(3232240993l));
    }
}
