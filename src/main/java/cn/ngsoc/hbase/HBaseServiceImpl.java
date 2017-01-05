package cn.ngsoc.hbase;

import cn.ngsoc.hbase.util.HBaseUtil;
import cn.ngsoc.hbase.util.ThreadPoolUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * HBaseService Mutator 实现类
 * Created by babylon on 2016/12/5.
 */
public class HBaseServiceImpl extends AbstractHBaseService{

    private static final Logger logger = LoggerFactory.getLogger(HBaseServiceImpl.class);

    private ThreadPoolUtil threadPool= ThreadPoolUtil.init();       // 初始化线程池

    @Override
    public void put(String tableName, Put put, boolean waiting) {
        batchPut(tableName, Arrays.asList(put), waiting);
     }

     /**
     * 多线程同步提交
     * @param tableName  表名称
     * @param puts  待提交参数
     * @param waiting  是否等待线程执行完成  true 可以及时看到结果, false 让线程继续执行，并跳出此方法返回调用方主程序
     */
    @Override
    public void batchPut(final String tableName, final List<Put> puts, boolean waiting) {
        threadPool.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    HBaseUtil.put(tableName, puts);
                } catch (Exception e) {
                    logger.error("batchPut failed . ", e);
                }
            }
        });

        if(waiting){
            try {
                threadPool.awaitTermination();
            } catch (InterruptedException e) {
                logger.error("HBase put job thread pool await termination time out.", e);
            }
        }
    }

    @Override
    public <T> Result[] getRows(String tablename, List<T> rows) {
        return HBaseUtil.getRows(tablename, rows);
    }

    @Override
    public Result getRow(String tablename, byte[] row) {
        return HBaseUtil.getRow(tablename, row);
    }

    /**
     * 多线程异步提交
     * @param tableName  表名称
     * @param puts  待提交参数
     * @param waiting  是否等待线程执行完成  true 可以及时看到结果, false 让线程继续执行，并跳出此方法返回调用方主程序
     */
    public void batchAsyncPut(final String tableName, final List<Put> puts, boolean waiting) {
        Future f = threadPool.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    HBaseUtil.putByHTable(tableName, puts);
                } catch (Exception e) {
                    logger.error("batchPut failed . ", e);
                }
            }
        });

        if(waiting){
            try {
                f.get();
            } catch (InterruptedException e) {
                logger.error("多线程异步提交返回数据执行失败.", e);
            } catch (ExecutionException e) {
                logger.error("多线程异步提交返回数据执行失败.", e);
            }
        }
    }

    /**
     * 创建表
     * @param tableName         表名称
     * @param columnFamilies   列族名称数组
     * @param preBuildRegion  是否预分配Region   true 是  ， false 否  默认 16个region，rowkey生成的时候记得指定前缀
     * @return  返回执行时间 (单位: 毫秒)
     */
    public void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) throws Exception {
        HBaseUtil.createTable(tableName, columnFamilies, preBuildRegion);
    }

}
