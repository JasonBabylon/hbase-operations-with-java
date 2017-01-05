import cn.ngsoc.hbase.util.HBaseUtil;
import cn.ngsoc.hbase.util.ThreadPoolUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 测试删除
 * Created by babylon on 2016/12/30.
 */
public class HBaseDeleteTest {

    private static final Logger logger = LoggerFactory.getLogger(HBaseDeleteTest.class);

    private static String TABLE_NAME = "logs";      // 表名
    private static long START_TIMESTAMP = 0;     // 数据开始时间
//    private static long END_TIMESTAMP = System.currentTimeMillis() - 5356800000L;    // 数据结束时间，2个月前
    private static long END_TIMESTAMP = System.currentTimeMillis() ;    // 数据结束时间
    private static int PAGE_SIZE = 100;     // 每次处理多少条数据
    private static int THREAD_COMSUME_PER_RECORDS = 10;     // 多线程每次处理多少数据
    private static String zkHost = "bd132:2181,bd133:2181,bd134:2181";
    private static String autoGc = "false";

    private static ThreadPoolUtil threadPool= ThreadPoolUtil.init();       // 初始化线程池

    private static AtomicLong count = new AtomicLong(0);

    private static List<Delete> listOfBatchDelete = new ArrayList<Delete>();

    private static BlockingQueue<byte[]> _rowKeys = new LinkedBlockingQueue<byte[]>();

    public static void main(String[] args) {
        try {

            if(args != null && args.length > 0){
                TABLE_NAME = args[0];
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date startDate = format.parse(args[1]);
                START_TIMESTAMP = startDate.getTime();
                Date endDate = format.parse(args[2]);
                END_TIMESTAMP = endDate.getTime();
                PAGE_SIZE = Integer.parseInt(args[3]);
                THREAD_COMSUME_PER_RECORDS = Integer.parseInt(args[4]);
                zkHost = args[5];
                autoGc = args[6];
            }

            HBaseUtil.init(zkHost);

            final Scan scan = new Scan();
            scan.setTimeRange(START_TIMESTAMP, END_TIMESTAMP);
            scan.setFilter(packageFilters());
            scan.setMaxResultSize(PAGE_SIZE);   // 分页返回
            final Table table = HBaseUtil.getTable(TABLE_NAME);

            while(true){

                ResultScanner scanner = table.getScanner(scan);
                final Iterator<Result> iterator = scanner.iterator();

                // 没有数据了，退出循环
                if(!iterator.hasNext()){
                    break;
                }

                logger.info("Readed " + PAGE_SIZE + " records, start delete.");
                while (iterator.hasNext()) {
                    Delete d=new Delete(iterator.next().getRow());
                    listOfBatchDelete.add(d);
                    count.incrementAndGet();
                    if(listOfBatchDelete.size() % THREAD_COMSUME_PER_RECORDS == 0){
//                        logger.info("Readed " + listOfBatchDelete.size() + " records, start delete.");
                        final List<Delete> tmp = new ArrayList<Delete>(listOfBatchDelete);
                        threadPool.execute(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    table.delete(tmp);
                                    tmp.clear();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                        listOfBatchDelete.clear();
                    }
                }
                // 如果本次循环还有剩余数据，一次性清理完
                if(listOfBatchDelete.size() > 0){
                    final List<Delete> tmp = new ArrayList<Delete>(listOfBatchDelete);
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                table.delete(tmp);
                                tmp.clear();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    });
                    listOfBatchDelete.clear();
                }
                // 剩余内存不足，促发GC
                long freeMem = Runtime.getRuntime().freeMemory()  / 1024 / 1024;
                if(autoGc.equalsIgnoreCase("true")){
                    if(freeMem < 500){
                        logger.info("Free memory total : " +freeMem + "mb, start gc.");
//                        System.out.println("Free memory total : " +freeMem + "mb, start gc.");
                        System.gc();
                    }
                }
                // 如果消费队列排到大于10了，那么停止生产，等待消费完
//                if(threadPool.getQueueSize() > 10){
                    threadPool.awaitTermination();
//                }
                listOfBatchDelete = null;
                listOfBatchDelete = new ArrayList<Delete>();
                logger.info("Deleted " + count.get() + " records now.");
//                System.out.println("Deleted " + count.get() + " records now.");
            }
            table.close();
            threadPool.awaitTermination();
            logger.info("Job comlete, deleted  " + count .get()+ " records.");
            System.out.println("Job comlete, deleted  " + count .get()+ " records.");
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 封装查询条件   */
    private static FilterList packageFilters() {
        FilterList filterList = null;
        // MUST_PASS_ALL(条件 AND) MUST_PASS_ONE（条件OR）
        filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//        Filter filter1 = null;
//        Filter filter2 = null;
//        filter1 = newFilter(Bytes.getBytes("family1"), Bytes.getBytes("column1"), CompareFilter.CompareOp.EQUAL, Bytes.getBytes("condition1"));
//        filter2 = newFilter(Bytes.getBytes("family2"), Bytes.getBytes("column1"), CompareFilter.CompareOp.LESS, Bytes.getBytes("condition2"));
//        filterList.addFilter(filter1);
//        filterList.addFilter(filter2);
        filterList.addFilter(new PageFilter(PAGE_SIZE));
        filterList.addFilter(new FirstKeyOnlyFilter());
        return filterList;
    }

    private static Filter newFilter(byte[] f, byte[] c, CompareFilter.CompareOp op, byte[] v) {
        return new SingleColumnValueFilter(f, c, op, v);
    }

}
