package cn.ngsoc.hbase.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by liguangting on 15-8-13.
 */
public class Md5Util {

    private static char[] digit = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

    public static String getHash(String plaintext) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(plaintext.getBytes());
            return byteToStr(digest);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return "";
    }

    private static String byteToStr(byte[] byteArray) {
        String rst = "";
        for (int i = 0; i < byteArray.length; i++) {
            rst += byteToHex(byteArray[i]);
        }
        return rst;
    }

    private static String byteToHex(byte b) {
        char[] tempArr = new char[2];
        tempArr[0] = digit[(b >>> 4) & 0X0F];
        tempArr[1] = digit[b & 0X0F];
        String s = new String(tempArr);
        return s;
    }
}
