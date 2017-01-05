package cn.ngsoc.hbase.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * HBase 工具类
 * Created by babylon on 2016/11/29.
 */
public class HBaseUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);

	private static Configuration conf;
	private static Connection conn;

//	static {
//		try {
//			if (conf == null) {
//			    conf = HBaseConfiguration.create();
////                conf.set("hbase.zookeeper.property.clientPort", ConfigUtil.getInstance().getConfigVal("zkport", ConstantProperties.COMMON_PROP));
//                conf.set("hbase.zookeeper.quorum", ConfigUtil.getInstance().getConfigVal("zkhost", ConstantProperties.COMMON_PROP));
//                conf.set("zookeeper.znode.parent", "/hbase");
//			}
//		} catch (Exception e) {
//			logger.error("HBase Configuration Initialization failure !");
//			throw new RuntimeException(e) ;
//		}
//	}

	public static void init(String zkHost){
		try {
			if (conf == null) {
				conf = HBaseConfiguration.create();
//                conf.set("hbase.zookeeper.property.clientPort", ConfigUtil.getInstance().getConfigVal("zkport", ConstantProperties.COMMON_PROP));
				conf.set("hbase.zookeeper.quorum", zkHost);
				conf.set("zookeeper.znode.parent", "/hbase");
			}
		} catch (Exception e) {
			logger.error("HBase Configuration Initialization failure !");
			throw new RuntimeException(e) ;
		}
	}

	/**
	 * 获得链接
	 * @return
     */
	public static synchronized Connection getConnection() {
		try {
            if(conn == null || conn.isClosed()){
                conn = ConnectionFactory.createConnection(conf);
            }
//         System.out.println("---------- " + conn.hashCode());
		} catch (IOException e) {
			logger.error("HBase 建立链接失败 ", e);
		}
		return conn;

	}

	/**
	 * 创建表
	 * @param tableName
	 * @throws Exception
     */
	public static void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) throws Exception {
		if(preBuildRegion){
			String[] s = new String[] { "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F" };
			int partition = 16;
			byte[][] splitKeys = new byte[partition - 1][];
			for (int i = 1; i < partition; i++) {
				splitKeys[i - 1] = Bytes.toBytes(s[i - 1]);
			}
			createTable(tableName, columnFamilies, splitKeys);
		} else {
			createTable(tableName, columnFamilies);
		}
	}

	private static void createTable(String tableName, int pNum, boolean only) throws Exception {
		String[] s = RandCodeEnum.HBASE_CHAR.getHbaseKeys(pNum,2,only);
		byte[][] splitKeys = new byte[pNum][];
		for (int i = 1; i <= pNum; i++) {
			splitKeys[i - 1] = Bytes.toBytes(s[i - 1]);
		}
		createTable(tableName, new String[] { "events" }, splitKeys);
	}
	
	/**
	 * 建表
	 * @param tableName
	 * @param cfs
	 * @throws IOException
	 */
	private static void createTable(String tableName, String[] cfs, byte[][] splitKeys) throws Exception {
		Connection conn = getConnection();
		HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
		try {
			if (admin.tableExists(tableName)) {
				logger.warn("Table: {} is exists!", tableName);
				return;
			}
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
			for (int i = 0; i < cfs.length; i++) {
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cfs[i]);
				hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
				hColumnDescriptor.setMaxVersions(1);
				tableDesc.addFamily(hColumnDescriptor);
			}
			admin.createTable(tableDesc, splitKeys);
			logger.info("Table: {} create success!", tableName);
		} finally {
			admin.close();
			closeConnect(conn);
		}
	}

	/**
	 * 建表
	 * @param tableName
	 * @param cfs
	 * @throws IOException
	 */
	private static void createTable(String tableName, String[] cfs) throws Exception {
		Connection conn = getConnection();
		HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
		try {
			if (admin.tableExists(tableName)) {
				logger.warn("Table: {} is exists!", tableName);
				return;
			}
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
			for (int i = 0; i < cfs.length; i++) {
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cfs[i]);
				hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
				hColumnDescriptor.setMaxVersions(1);
				tableDesc.addFamily(hColumnDescriptor);
			}
			admin.createTable(tableDesc);
			logger.info("Table: {} create success!", tableName);
		} finally {
			admin.close();
			closeConnect(conn);
		}
	}

    /**
     * 删除表
     * @param tablename
     * @throws IOException
     */
	public static void deleteTable(String tablename) throws IOException {
		Connection conn = getConnection();
		HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
		try {
			if (!admin.tableExists(tablename)) {
				logger.warn("Table: {} is not exists!", tablename);
				return;
			}
			admin.disableTable(tablename);
			admin.deleteTable(tablename);
			logger.info("Table: {} delete success!", tablename);
		} finally {
			admin.close();
			closeConnect(conn);
		}
	}
	
	/**
	 * 获取  Table
	 * @param tableName 表名
	 * @return
	 * @throws IOException
	 */
	public static Table getTable(String tableName){
		try {
			return getConnection().getTable(TableName.valueOf(tableName));
		} catch (Exception e) {
			logger.error("Obtain Table failure !", e);
		}
		return null;
	}

	/**
	 * 给 table 创建 snapshot
	 * @param snapshotName 快照名称
     * @param tableName 表名
	 * @return
	 * @throws IOException
	 */
	public static void snapshot(String snapshotName, TableName tableName){
		try {
            Admin admin = getConnection().getAdmin();
            admin.snapshot(snapshotName, tableName);
		} catch (Exception e) {
			logger.error("Snapshot " + snapshotName + " create failed !", e);
		}
	}

    /**
     * 获得现已有的快照
     * @param snapshotNameRegex 正则过滤表达式
     * @return
     * @throws IOException
     */
    public static List<HBaseProtos.SnapshotDescription> listSnapshots(String snapshotNameRegex){
        try {
            Admin admin = getConnection().getAdmin();
            if(StringUtils.isNotBlank(snapshotNameRegex))
                return admin.listSnapshots(snapshotNameRegex);
            else
                return admin.listSnapshots();
        } catch (Exception e) {
            logger.error("Snapshot " + snapshotNameRegex + " get failed !", e);
        }
        return null;
    }

    /**
     * 批量删除Snapshot
     * @param snapshotNameRegex 正则过滤表达式
     * @return
     * @throws IOException
     */
    public static void deleteSnapshots(String snapshotNameRegex){
        try {
            Admin admin = getConnection().getAdmin();
            if(StringUtils.isNotBlank(snapshotNameRegex))
                admin.deleteSnapshots(snapshotNameRegex);
            else
                logger.error("SnapshotNameRegex can't be null !");
        } catch (Exception e) {
            logger.error("Snapshots " + snapshotNameRegex + " del failed !", e);
        }
    }

    /**
     * 单个删除Snapshot
     * @param snapshotName 正则过滤表达式
     * @return
     * @throws IOException
     */
    public static void deleteSnapshot(String snapshotName){
        try {
            Admin admin = getConnection().getAdmin();
            if(StringUtils.isNotBlank(snapshotName))
                admin.deleteSnapshot(snapshotName);
            else
                logger.error("SnapshotName can't be null !");
        } catch (Exception e) {
            logger.error("Snapshot " + snapshotName + " del failed !", e);
        }
    }

	/**
	 * 分页检索表数据。<br>
	 * （如果在创建表时为此表指定了非默认的命名空间，则需拼写上命名空间名称，格式为【namespace:tablename】）。
	 * @param tableName 表名称(*)。
	 * @param startRowKey 起始行键(可以为空，如果为空，则从表中第一行开始检索)。
	 * @param endRowKey 结束行键(可以为空)。
	 * @param filterList 检索条件过滤器集合(不包含分页过滤器；可以为空)。
	 * @param maxVersions 指定最大版本数【如果为最大整数值，则检索所有版本；如果为最小整数值，则检索最新版本；否则只检索指定的版本数】。
	 * @param pageModel 分页模型(*)。
	 * @return 返回HBasePageModel分页对象。
	 */
	public static HBasePageModel scanResultByPageFilter(String tableName, byte[] startRowKey, byte[] endRowKey, FilterList filterList, int maxVersions, HBasePageModel pageModel) {
		if(pageModel == null) {
			pageModel = new HBasePageModel(10);
		}
		if(maxVersions <= 0 ) {
			//默认只检索数据的最新版本
			maxVersions = Integer.MIN_VALUE;
		}
		pageModel.initStartTime();
		pageModel.initEndTime();
		if(StringUtils.isBlank(tableName)) {
			return pageModel;
		}
		Table table = null;

		try {
			table = getTable(tableName);
			int tempPageSize = pageModel.getPageSize();
			boolean isEmptyStartRowKey = false;
			if(startRowKey == null) {
				//则读取表的第一行记录
				Result firstResult = selectFirstResultRow(tableName, filterList);
				if(firstResult.isEmpty()) {
					return pageModel;
				}
				startRowKey = firstResult.getRow();
			}
			if(pageModel.getPageStartRowKey() == null) {
				isEmptyStartRowKey = true;
				pageModel.setPageStartRowKey(startRowKey);
			} else {
				if(pageModel.getPageEndRowKey() != null) {
					pageModel.setPageStartRowKey(pageModel.getPageEndRowKey());
				}
				//从第二页开始，每次都多取一条记录，因为第一条记录是要删除的。
				tempPageSize += 1;
			}

			Scan scan = new Scan();
			scan.setStartRow(pageModel.getPageStartRowKey());
			if(endRowKey != null) {
				scan.setStopRow(endRowKey);
			}
			PageFilter pageFilter = new PageFilter(pageModel.getPageSize() + 1);
			if(filterList != null) {
				filterList.addFilter(pageFilter);
				scan.setFilter(filterList);
			} else {
				scan.setFilter(pageFilter);
			}
			if(maxVersions == Integer.MAX_VALUE) {
				scan.setMaxVersions();
			} else if(maxVersions == Integer.MIN_VALUE) {

			} else {
				scan.setMaxVersions(maxVersions);
			}
			ResultScanner scanner = table.getScanner(scan);
			List<Result> resultList = new ArrayList<Result>();
			int index = 0;
			for(Result rs : scanner.next(tempPageSize)) {
				if(isEmptyStartRowKey == false && index == 0) {
					index += 1;
					continue;
				}
				if(!rs.isEmpty()) {
					resultList.add(rs);
				}
				index += 1;
			}
			scanner.close();
			pageModel.setResultList(resultList);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		int pageIndex = pageModel.getPageIndex() + 1;
		pageModel.setPageIndex(pageIndex);
		if(pageModel.getResultList().size() > 0) {
			//获取本次分页数据首行和末行的行键信息
			byte[] pageStartRowKey = pageModel.getResultList().get(0).getRow();
			byte[] pageEndRowKey = pageModel.getResultList().get(pageModel.getResultList().size() - 1).getRow();
			pageModel.setPageStartRowKey(pageStartRowKey);
			pageModel.setPageEndRowKey(pageEndRowKey);
		}
		int queryTotalCount = pageModel.getQueryTotalCount() + pageModel.getResultList().size();
		pageModel.setQueryTotalCount(queryTotalCount);
		pageModel.initEndTime();
		pageModel.printTimeInfo();
		return pageModel;
	}

	/**
	 * 检索指定表的第一行记录。<br>
	 * （如果在创建表时为此表指定了非默认的命名空间，则需拼写上命名空间名称，格式为【namespace:tablename】）。
	 * @param tableName 表名称(*)。
	 * @param filterList 过滤器集合，可以为null。
	 * @return
	 */
	public static Result selectFirstResultRow(String tableName,FilterList filterList) {
		if(StringUtils.isBlank(tableName)) return null;
		Table table = null;
		try {
			table = getTable(tableName);
			Scan scan = new Scan();
			if(filterList != null) {
				scan.setFilter(filterList);
			}
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> iterator = scanner.iterator();
			int index = 0;
			while(iterator.hasNext()) {
				Result rs = iterator.next();
				if(index == 0) {
					scanner.close();
					return rs;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
    /**
     * 异步往指定表添加数据
     * @param tablename  	表名
     * @param puts	 			需要添加的数据
	 * @return long				返回执行时间
     * @throws IOException
     */
	public static long put(String tablename, List<Put> puts) throws Exception {
		long currentTime = System.currentTimeMillis();
		Connection conn = getConnection();
		final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
			@Override
			public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
				for (int i = 0; i < e.getNumExceptions(); i++) {
					System.out.println("Failed to sent put " + e.getRow(i) + ".");
					logger.error("Failed to sent put " + e.getRow(i) + ".");
				}
			}
		};
		BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tablename))
				.listener(listener);
		params.writeBufferSize(5 * 1024 * 1024);

		final BufferedMutator mutator = conn.getBufferedMutator(params);
		try {
			mutator.mutate(puts);
			mutator.flush();
		} finally {
			mutator.close();
			closeConnect(conn);
		}
		return System.currentTimeMillis() - currentTime;
	}

	/**
	 * 异步往指定表添加数据
	 * @param tablename  	表名
	 * @param put	 			需要添加的数据
	 * @return long				返回执行时间
	 * @throws IOException
	 */
	public static long put(String tablename, Put put) throws Exception {
		return put(tablename, Arrays.asList(put));
	}

	/**
	 * 往指定表添加数据
	 * @param tablename  	表名
	 * @param puts	 			需要添加的数据
	 * @return long				返回执行时间
	 * @throws IOException
	 */
	public static long putByHTable(String tablename, List<?> puts) throws Exception {
		long currentTime = System.currentTimeMillis();
		Connection conn = getConnection();
        HTable htable = (HTable) conn.getTable(TableName.valueOf(tablename));
		htable.setAutoFlushTo(false);
		htable.setWriteBufferSize(5 * 1024 * 1024);
		try {
			htable.put((List<Put>)puts);
			htable.flushCommits();
		} finally {
			htable.close();
			closeConnect(conn);
		}
		return System.currentTimeMillis() - currentTime;
	}
    
	/**
	 * 删除单条数据
	 * @param tablename
	 * @param row
	 * @throws IOException
	 */
	public static void delete(String tablename, String row) throws IOException {
		Table table = getTable(tablename);
        if(table!=null){
			try {
				Delete d = new Delete(row.getBytes());
				table.delete(d);
			} finally {
				table.close();
			}
        }
	}

	/**
	 * 删除多行数据
	 * @param tablename
	 * @param rows
	 * @throws IOException
	 */
	public static void delete(String tablename, String[] rows) throws IOException {
		Table table = getTable(tablename);
		if (table != null) {
			try {
				List<Delete> list = new ArrayList<Delete>();
				for (String row : rows) {
					Delete d = new Delete(row.getBytes());
					list.add(d);
				}
				if (list.size() > 0) {
					table.delete(list);
				}
			} finally {
				table.close();
			}
		}
	}

	/**
	 * 关闭连接
	 * @throws IOException
	 */
	public static void closeConnect(Connection conn){
		if(null != conn){
			try {
//				conn.close();
			} catch (Exception e) {
                logger.error("closeConnect failure !", e);
			}
		}
	}

	/**
	 * 获取单条数据
	 * @param tablename
	 * @param row
	 * @return
	 * @throws IOException
	 */
	public static Result getRow(String tablename, byte[] row) {
		Table table = getTable(tablename);
		Result rs = null;
		if(table!=null){
			try{
				Get g = new Get(row);
				rs = table.get(g);
			} catch (IOException e) {
                logger.error("getRow failure !", e);
			} finally{
				try {
					table.close();
				} catch (IOException e) {
                    logger.error("getRow failure !", e);
				}
			}
		}
		return rs;
	}

	/**
	 * 获取多行数据
	 * @param tablename
	 * @param rows
	 * @return
	 * @throws Exception
	 */
	public static <T> Result[] getRows(String tablename, List<T> rows) {
        Table table = getTable(tablename);
        List<Get> gets = null;
        Result[] results = null;
        try {
            if (table != null) {
                gets = new ArrayList<Get>();
                for (T row : rows) {
                    if(row!=null){
                        gets.add(new Get(Bytes.toBytes(String.valueOf(row))));
                    }else{
                        throw new RuntimeException("hbase have no data");
                    }
                }
            }
            if (gets.size() > 0) {
                results = table.get(gets);
            }
        } catch (IOException e) {
            logger.error("getRows failure !", e);
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                logger.error("table.close() failure !", e);
            }
        }
		return results;
	}

	/**
	 * 扫描整张表，注意使用完要释放。
	 * @param tablename
	 * @return
	 * @throws IOException
	 */
	public static ResultScanner get(String tablename) {
		Table table = getTable(tablename);
		ResultScanner results = null;
		if (table != null) {
			try {
				Scan scan = new Scan();
				scan.setCaching(1000);
				results = table.getScanner(scan);
			} catch (IOException e) {
                logger.error("getResultScanner failure !", e);
			} finally {
				try {
					table.close();
				} catch (IOException e) {
                    logger.error("table.close() failure !", e);
				}
			}
		}
		return results;
	}

	/**
	 * 格式化输出结果
	 */
	public static void formatRow(KeyValue[] rs){
		for(KeyValue kv : rs){
			System.out.println(" column family  :  " + Bytes.toString(kv.getFamily()));
			System.out.println(" column   :  " + Bytes.toString(kv.getQualifier()));
			System.out.println(" value   :  " + Bytes.toString(kv.getValue()));
			System.out.println(" timestamp   :  " + String.valueOf(kv.getTimestamp()));
			System.out.println("--------------------");
		}
	}

	/**
	 * byte[] 类型的长整形数字转换成 long 类型
	 * @param byteNum
	 * @return
	 */
	public static long bytes2Long(byte[] byteNum) {
		long num = 0;
		for (int ix = 0; ix < 8; ++ix) {
			num <<= 8;
			num |= (byteNum[ix] & 0xff);
		}
		return num;
	}

}