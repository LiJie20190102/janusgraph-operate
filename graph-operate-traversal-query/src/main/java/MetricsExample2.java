import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.io.File;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Description
 *
 * @author lijie0203 2024/3/4 22:21
 */
public class MetricsExample2 {
    private static final MetricRegistry metrics = new MetricRegistry();
    private static final Timer timer = metrics.timer("requestaaaa");

    public static void main(String[] args) {
        CsvReporter reporter = CsvReporter.forRegistry(metrics).formatFor(Locale.US)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.SECONDS)
                .build(new File("D:/tmp/1031/csv_test/"));
        reporter.start(1, TimeUnit.SECONDS);

        // 模拟一些请求
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j <=i; j++) {
            Timer.Context context = timer.time();
            try {
                // 模拟请求处理耗时
                int i1 = 5 - i;
                TimeUnit.SECONDS.sleep(i1);
            } catch (InterruptedException e) {
                // 处理异常
            } finally {
                context.stop();
            }
            }
        }
    }
}
