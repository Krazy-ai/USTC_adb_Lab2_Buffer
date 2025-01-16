package ustc.krazy;

import ustc.krazy.check.Trace;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author krazy
 * @date 2025/1/6
 */
public class DemonstrateRes {
    public static void main(String[] args) throws FileNotFoundException {
        Trace trace = new Trace();
        try {
            trace.createFile();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("创建文件失败");
        }
        try {
            double startTime = System.currentTimeMillis();
            trace.getStatistics();
            double endTime = System.currentTimeMillis();
            double runTime = endTime - startTime;
            System.out.printf("内存和磁盘之间的总I/O次数: %d\n" +
                    " 缓存命中率: %.3f%%\n" +
                    " 测试运行时间: %.3fs\n", trace.getIOCounter(), trace.getHitRate() * 100, (double)runTime / 1000);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("测试程序异常!");
        }
    }
}