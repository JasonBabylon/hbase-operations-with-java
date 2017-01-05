package cn.ngsoc.hbase.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by liguangting on 15-11-12.
 */
public class PerformanceTest {
    private Map<String, Stat> total = new HashMap<String, Stat>();
    private Map<String, Stat> last = new HashMap<String, Stat>();
    int count = 0;

    private void statNoLock(String key, long time) {
        Stat total_value = total.get(key);
        if (total_value != null) {
            total_value.times = total_value.times + 1;
            total_value.total = total_value.total + time;
            total_value.avg = total_value.total / total_value.times;
            if (time > total_value.max) total_value.max = time;
            if (time < total_value.min) total_value.min = time;
        } else {
            total.put(key, new Stat(time, 1, time, time, time));
        }

        Stat last_value = last.get(key);
        if (last_value != null) {
            last_value.times = last_value.times + 1;
            last_value.total = last_value.total + time;
            last_value.avg = last_value.total / last_value.times;
            if (time > last_value.max) last_value.max = time;
            if (time < last_value.min) last_value.min = time;
        } else {
            last.put(key, new Stat(time, 1, time, time, time));
        }
    }

    public void statWithLock(String key, long time) {
        synchronized(this) {
            statNoLock(key, time);
        }
    }

    public void stat(Map<String, Long> kv) {
        synchronized(this) {
            for (Map.Entry<String, Long> entry : kv.entrySet()) {
                statNoLock(entry.getKey(), entry.getValue());
            }
        }
    }

    public void print() {
        synchronized(this) {
            count = count + 1;
            if (count % 10 == 0) {
                System.out.printf("%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s%-10s\n",
                        "=key=", "=Tavg=", "=Ttimes=", "=Ttotal=", "=Tmin=", "=Tmax=", "=Lavg=", "=Ltimes=", "=Ltotal=", "=Lmin=", "=Lmax=");
                for (Map.Entry<String, Stat> kv : total.entrySet()) {
                    Stat value = last.get(kv.getKey());
                    System.out.printf("%-10s%-10d%-10d%-10d%-10d%-10d",
                            kv.getKey(), kv.getValue().avg, kv.getValue().times, kv.getValue().total, kv.getValue().min, kv.getValue().max);
                    if (value != null) {
                        System.out.printf("%-10d%-10d%-10d%-10d%-10d", value.avg, value.times, value.total, value.min, value.max);
                    }
                    System.out.println();
                }
                // 清空last_map的数据
                last.clear();
            }
        }
    }
}

class Stat {
    public long avg;
    public int times;
    public long total;
    public long min;
    public long max;

    public Stat(long avg, int times, long total, long min, long max) {
        this.avg= avg;
        this.times = times;
        this.total = total;
        this.min = min;
        this.max = max;
    }
}