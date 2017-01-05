package cn.ngsoc.hbase;

import cn.ngsoc.hbase.util.Md5Util;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.List;

/**
 * HBase 各个组件管理调用类
 * 可以根据配置文件来选择  HBase 官方 API 还是第三方API
 * Created by babylon on 2016/12/6.
 */
public class HBase {

    private HBase() {}

//    private static volatile HBase instance = null;
    private static HBaseService hBaseService;

//    public static HBase getInstance() {
//        if (instance == null) {
//            synchronized (HBase.class) {
//                if (instance == null) {
//                    instance = new HBase();
//                }
//            }
//        }
//        return instance;
//    }

    static {
        // TODO  根据配置文件来选择  HBase 官方 API 还是第三方API
        hBaseService = new HBaseServiceImpl();
//        hBaseService = new AsyncHBaseServiceImpl();
    }

    /**
     * 写入单条数据
     * @param tableName   表名称
     * @param put              列值
     * @param waiting  是否等待线程执行完成  true 可以及时看到结果, false 让线程继续执行，并跳出此方法返回调用方主程序
     * @return
     */
    public static void put(String tableName, Put put, boolean waiting) {
        hBaseService.batchPut(tableName, Arrays.asList(put), waiting);
    }

    /**
     * 多线程同步提交
     * @param tableName  表名称
     * @param puts  待提交参数
     * @param waiting  是否等待线程执行完成  true 可以及时看到结果, false 让线程继续执行，并跳出此方法返回调用方主程序
     */
    public static void put(String tableName, List<Object> puts, boolean waiting) {
        hBaseService.batchPut(tableName, puts, waiting);
    }

    /**
     * 获取多行数据
     * @param tablename
     * @param rows
     * @return
     * @throws Exception
     */
    public static <T> Result[] getRows(String tablename, List<T> rows) throws Exception {
        return hBaseService.getRows(tablename, rows);
    }

    /**
     * 获取单条数据
     * @param tablename
     * @param row
     * @return
     */
    public static Result getRow(String tablename, byte[] row) {
        return hBaseService.getRow(tablename, row);
    }

    public static <T> byte[] generateRowkey(T rowKey){
        // TODO 测试generateRowkey
        return Bytes.toBytes(Md5Util.getHash(String.valueOf(rowKey)).substring(0, 8) + "_" + String.valueOf(rowKey));
    }

}
