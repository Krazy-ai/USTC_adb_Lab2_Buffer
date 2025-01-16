package ustc.krazy;

import ustc.krazy.check.TraceTask;

import java.io.FileNotFoundException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;

/**
 * @author krazy
 * @date 2025/1/6
 */
public class DemonstrateRes {
    public static void main(String[] args) throws FileNotFoundException {
        int numberOfThreads = 3;

        // 创建一个固定大小的线程池
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        // 提交任务到线程池
        for (int i = 1; i <= numberOfThreads; i++) {
            TraceTask task = new TraceTask(i);
            executor.submit(task);
        }
        // 关闭线程池，等待所有任务完成
        try {
            sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                executor.shutdownNow(); // 超时后强制关闭
                System.out.println("线程池未能在规定时间内关闭.");
            } else {
                System.out.println("所有任务已完成.");
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            e.printStackTrace();
            System.out.println("线程池关闭时发生中断.");
        }
    }
}