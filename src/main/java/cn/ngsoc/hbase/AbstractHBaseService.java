package cn.ngsoc.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;

/**
 * HBase 服务抽象类
 * Created by babylon on 2016/12/6.
 */

public abstract class AbstractHBaseService implements HBaseService {

    @Override
    public void put(String tableName, Put put, boolean waiting) {}

    @Override
    public void batchPut(final String tableName, final List<Put> puts, boolean waiting) {}

    @Override
    public <T> Result[] getRows(String tablename, List<T> rows) {return null;}

    @Override
    public Result getRow(String tablename, byte[] row) {return null;}

}
