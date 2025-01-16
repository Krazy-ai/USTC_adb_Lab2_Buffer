package ustc.krazy.check;

import java.io.IOException;

/**
 * @author krazy
 * @date 2025/1/16
 */
public class TraceTask implements Runnable {
    private final int taskId;

    public TraceTask(int taskId) {
        this.taskId = taskId;
    }

    @Override
    public void run() {
        long threadId = Thread.currentThread().getId();
        System.out.println("Task " + taskId + " - Thread ID: " + threadId);

        Trace trace = new Trace();
        try {
            trace.createFile();
            System.out.println("Task " + taskId + " - 文件创建成功");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Task " + taskId + " - 创建文件失败");
            return;
        }

        try {
            double startTime = System.currentTimeMillis();
            trace.getStatistics();
            double endTime = System.currentTimeMillis();
            double runTime = endTime - startTime;
            System.out.printf("Task %d - 内存和磁盘之间的总I/O次数: %d\n" +
                            "Task %d - 缓存命中率: %.3f%%\n" +
                            "Task %d - 测试运行时间: %.3fs\n",
                    taskId,
                    trace.getIOCounter(),
                    taskId,
                    trace.getHitRate() * 100,
                    taskId,
                    runTime / 1000);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Task " + taskId + " - 测试程序异常!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

