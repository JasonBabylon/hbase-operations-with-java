package cn.ngsoc.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;

/**
 * HBase 服务接口类
 * Created by babylon on 2016/12/4.
 */
public interface HBaseService {

    /**
     * 创建表
     * @param tableName         表名称
     * @param columnFamilies   列族名称数组
     * @param preBuildRegion  是否预分配Region   true 是  ， false 否  默认 16个region，rowkey生成的时候记得指定前缀
     * @return  返回执行时间 (单位: 毫秒)
     */
//    public void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) throws Exception;

    /**
     * 写入数据
     * @param tableName   表名称
     * @param put              列值
     * @param waiting  是否等待线程执行完成  true 可以及时看到结果, false 让线程继续执行，并跳出此方法返回调用方主程序
     * @return
     */
    public void put(String tableName, Put put, boolean waiting);

    /**
     * 批量写入数据
     * @param tableName  表名称
     * @param puts         Put 类型的列表
     * @param waiting  是否等待线程执行完成  true 可以及时看到结果, false 让线程继续执行，并跳出此方法返回调用方主程序
     * @return
     */
    public void batchPut(String tableName, final List<Put> puts, boolean waiting);

    <T> Result[] getRows(String tablename, List<T> rows);

    Result getRow(String tablename, byte[] row);
}
