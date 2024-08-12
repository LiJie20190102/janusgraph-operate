import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.util.concurrent.TimeUnit;

/**
 * Description
 *
 * @author lijie0203 2024/3/4 22:21
 */
public class MetricsExample {
    private static final MetricRegistry metrics = new MetricRegistry();
    private static final Timer timer = metrics.timer("request");

    public static void main(String[] args) {
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.SECONDS)
                .build();
        reporter.start(2, TimeUnit.SECONDS);

        // 模拟一些请求
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j <=i; j++) {
            Timer.Context context = timer.time();
            try {
                // 模拟请求处理耗时
//                int i1 = 5 - i;
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                // 处理异常
            } finally {
                context.stop();
            }
            }
        }
    }
}
