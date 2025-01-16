package ustc.krazy.check;

import lombok.Data;
import ustc.krazy.constant.Permissions;
import ustc.krazy.manager.BufferManager;
import ustc.krazy.manager.DataStorageManager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static ustc.krazy.constant.BufferConstants.FRAME_SIZE;
import static ustc.krazy.constant.BufferConstants.MAX_PAGES;

/**
 * @author krazy
 * @date 2025/1/6
 */
@Data
public class Trace {
    private final BufferManager bufferManager = new BufferManager();
    private long threadId = Thread.currentThread().getId();
    public Map<Long, Integer> IOCounter = new HashMap<>();
    public Map<Long, Double> hitRate = new HashMap<>();
    public void createFile() throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile("data.dbf","rw");
        byte[] buf = new byte[FRAME_SIZE];
        for (int i = 0; i < FRAME_SIZE; i++) {
            buf[i] = '1';}
        for (int j = 0; j < MAX_PAGES; j++) {
            randomAccessFile.write(buf);
        }
        randomAccessFile.close();
    }

    public int read(int pageId) throws Exception {
        bufferManager.fixPage(pageId, Permissions.READ_ONLY);
        bufferManager.unFixPage(pageId);
        return BufferManager.hitCounter.getOrDefault(threadId,0);
    }

    public int write(int pageId) throws Exception {
        bufferManager.setDirty(bufferManager.fixPage(pageId,Permissions.READ_WRITE));
        bufferManager.unFixPage(pageId);
        return BufferManager.hitCounter.getOrDefault(threadId,0);
    }
    public void getStatistics() throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/data-5w-50w-zipf.txt"));
        String tmp = null;
        ArrayList<String> list = new ArrayList<>();
        while((tmp = reader.readLine()) != null) {
            tmp = tmp.trim();
            if(tmp.length() > 0) {
                list.add(tmp);
            }
        }
        reader.close();
        for(var line : list) {
            String[] tmpArray = line.split(",");
            int op = Integer.parseInt(tmpArray[0].toString());
            int pageId = Integer.parseInt(tmpArray[1].toString()) - 1;
            if(op == 0) {
                this.read(pageId);
            } else {
                this.write(pageId);
            }
        }
        this.end();
        this.hitRate.put(threadId, (double) BufferManager.hitCounter.getOrDefault(threadId,0) / list.size());
        this.IOCounter.put(threadId,
                DataStorageManager.ICounter.getOrDefault(threadId,0) + DataStorageManager.OCounter.getOrDefault(threadId,0));
    }

    private void end() {
        try {
            bufferManager.writeDirtys();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("测试程序退出异常");
        }
    }

    public int getIOCounter(){
        return IOCounter.getOrDefault(threadId,0);
    }

    public double getHitRate(){
        return hitRate.getOrDefault(threadId,0.0);
    }

}
