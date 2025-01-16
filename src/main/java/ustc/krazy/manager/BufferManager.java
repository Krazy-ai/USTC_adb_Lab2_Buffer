package ustc.krazy.manager;

import ustc.krazy.constant.Permissions;
import ustc.krazy.structure.BufferControlBlocks;
import ustc.krazy.structure.LRU;
import ustc.krazy.structure.bFrame;
import ustc.krazy.lock.PageLockManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import static ustc.krazy.constant.BufferConstants.*;

/**
 * @author krazy
 * @date 2025/1/6
 */
public class BufferManager {
    private long threadId = Thread.currentThread().getId();
    private PageLockManager pageLockManager;
    //frameId和frameId的映射关系
    private final int[] ftop = new int[DEF_BUFFER_SIZE];
    private final BufferControlBlocks[] ptof = new BufferControlBlocks[DEF_BUFFER_SIZE];
    private final Map<Long, Integer> tRandomTimeout = new ConcurrentHashMap<>();//并发
    private static final int TIMEOUT_MILLISECONDS = 1500; // 认为死锁超时的时间
    private final Random randomTimeout = new Random();
    public final bFrame[] buf = new bFrame[DEF_BUFFER_SIZE];
    private final DataStorageManager dataStorageManager = new DataStorageManager();

    //LRU链表
    private LRU head;
    private LRU tail;
    public static Map<Long, Integer> hitCounter;

    public BufferManager(){
        for(int i = 0;i < DEF_BUFFER_SIZE;i++) {
            this.ptof[i] = null;
            this.ftop[i] = -1;
        }
        int success = this.dataStorageManager.openFile("data.dbf");
        if(success == 0) {
            System.out.println("打开文件异常");
        }
        hitCounter = new HashMap<>();
        pageLockManager = new PageLockManager();
    }

    @Override
    protected void finalize() throws Throwable {
        dataStorageManager.closeFile();
    }

    /**
     *
     * @param pageId
     * @param perm
     * @return
     */
    public int fixPage(int pageId, Permissions perm) throws Exception {
        int acquireType = perm == Permissions.READ_ONLY ? PageLockManager.PageLock.SHARE : PageLockManager.PageLock.EXCLUSIVE;
        long start = System.currentTimeMillis();
        while (true) {
            try {
                if (pageLockManager.acquireLock(pageId, threadId, acquireType)) {
                    BufferControlBlocks BufferControlBlocks = this.ptof[this.hash(pageId)];
                    while(BufferControlBlocks != null && BufferControlBlocks.pageId != pageId) {
                        BufferControlBlocks = BufferControlBlocks.next;
                    }
                    if(BufferControlBlocks != null) {
                        hitCounter.merge(threadId, 1, Integer::sum);
                        LRU p = this.getLRU(BufferControlBlocks.frameId);
                        if(p == null) {
                            throw new RuntimeException("buffer命中了，但是LRU链表中找不到对应的结点");
                        } else{
                            // p不在表尾
                            if(p.next != null) {
                                if(p.pre == null) {
                                    // 在开头
                                    this.head = p.next;
                                    this.head.pre = null;
                                    p.next = null;
                                    this.tail.next = p;
                                    p.pre = this.tail;
                                    p.next = null;
                                    this.tail = p;
                                } else {
                                    p.pre.next = p.next;
                                    p.next.pre = p.pre;
                                    this.tail.next = p;
                                    p.pre = this.tail;
                                    p.next = null;
                                    this.tail = p;
                                }
                            }
                        }
                        BufferControlBlocks.count++;
                        return BufferControlBlocks.frameId;
                    }
                    //缓存未命中
                    int victimFrameId = this.selectVictim();
                    BufferControlBlocks nowBufferControlBlocks = new BufferControlBlocks();
                    nowBufferControlBlocks.pageId = pageId;
                    nowBufferControlBlocks.frameId = victimFrameId;
                    nowBufferControlBlocks.count++;

                    if(ftop[victimFrameId] != -1) {
                        BufferControlBlocks victimBufferControlBlocks = ptof[hash(ftop[victimFrameId])];
                        while(victimBufferControlBlocks != null && victimBufferControlBlocks.frameId != victimFrameId) {
                            victimBufferControlBlocks = victimBufferControlBlocks.next;
                        }
                        if(victimBufferControlBlocks == null) {
                            throw new RuntimeException("selectVictim未找到对应的页帧");
                        }
                        this.removeBufferControlBlocks(victimBufferControlBlocks,victimBufferControlBlocks.pageId);
                        this.ftop[victimBufferControlBlocks.frameId] = -1;
                        this.removeLRU(victimBufferControlBlocks.frameId);
                    }

                    this.ftop[nowBufferControlBlocks.frameId] = nowBufferControlBlocks.pageId;
                    BufferControlBlocks tmpBufferControlBlocks = this.ptof[this.hash(nowBufferControlBlocks.pageId)];
                    if(tmpBufferControlBlocks == null) {
                        this.ptof[this.hash(nowBufferControlBlocks.pageId)] = nowBufferControlBlocks;
                    } else {
                        while(tmpBufferControlBlocks.next != null) {
                            tmpBufferControlBlocks = tmpBufferControlBlocks.next;
                        }
                        tmpBufferControlBlocks.next = nowBufferControlBlocks;
                    }

                    LRU node = new LRU();
                    node.bcb = nowBufferControlBlocks;
                    if(this.head == null && this.tail == null) {
                        this.head = node;
                        this.tail = node;
                    } else {
                        this.tail.next = node;
                        node.pre = this.tail;
                        node.next = null;
                        this.tail = node;
                    }

                    try {
                        if(perm == Permissions.READ_ONLY ) {
                            this.buf[nowBufferControlBlocks.frameId] = dataStorageManager.readPage(nowBufferControlBlocks.pageId);
                        } else {
                            this.buf[nowBufferControlBlocks.frameId] = dataStorageManager.writePage(nowBufferControlBlocks.pageId,new bFrame(new byte[FRAME_SIZE]));
                        }
                    } catch (IOException e) {
                        throw new RuntimeException("读入调入页面异常");
                    }
                    return nowBufferControlBlocks.frameId;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // 如果未能获取到锁，判断是否超时
            long now = System.currentTimeMillis();
            if (now - start > getTransactionTimeout(threadId)) {
                throw new Exception();
            }
            //添加短暂的休眠，避免 CPU 过度占用
            // 随机化减少竞争
            try {
                Thread.sleep(20 + ThreadLocalRandom.current().nextInt(60));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();// 中断处理
            }
        }

    }

    // 随机化超时时间
    private int getTransactionTimeout(long tid) {
        tRandomTimeout.putIfAbsent(tid, randomTimeout.nextInt(1000) + TIMEOUT_MILLISECONDS);
        Integer res = tRandomTimeout.get(tid); // 以防并发问题
        return res == null ? TIMEOUT_MILLISECONDS : res;
    }

    public int fixNewPage() throws Exception {
        int[] pages = dataStorageManager.getPages();
        if(dataStorageManager.getNumPages() == pages.length) {
            return -1;
        }
        for(int pageId = 0; pageId < pages.length; pageId++) {
            if(pages[pageId] == 0) {
                dataStorageManager.setUse(pageId,MAX_PAGES);
                dataStorageManager.incNumPages();
                fixPage(pageId,Permissions.READ_ONLY);
                return pageId;
            }
        }
        return -1;
    }

    public int unFixPage(int pageId) {
        BufferControlBlocks BufferControlBlocks = ptof[this.hash(pageId)];
        while(BufferControlBlocks != null && BufferControlBlocks.pageId != pageId) {
            BufferControlBlocks = BufferControlBlocks.next;
        }
        if(BufferControlBlocks == null) {
            pageLockManager.releaseLock(pageId,threadId);
            return -1;
        }
        else {
            BufferControlBlocks.count --;
            return BufferControlBlocks.frameId;
        }
    }

    public int numFreeFrames() {
        int i = 0;
        while(i < DEF_BUFFER_SIZE && ftop[i] != -1) {
            ++i;
        }
        if(i == DEF_BUFFER_SIZE) {
            return -1;
        } else {
            return i;
        }
    }

    public int selectVictim() {
        if(this.numFreeFrames() != -1) {
            return this.numFreeFrames();
        } else {
            LRU p = this.head;
            while(p.bcb.count != 0) {
                p = p.next;
            }
            return p.bcb.frameId;
        }

    }

    public int hash(int pageId) {
        return pageId % DEF_BUFFER_SIZE;
    }

    public void removeBufferControlBlocks(BufferControlBlocks ptr, int pageId) {
        BufferControlBlocks BufferControlBlocks = this.ptof[this.hash(pageId)];
        if(BufferControlBlocks == null) {
            return;
        }
        if(BufferControlBlocks == ptr) {
            this.ptof[this.hash(pageId)] = BufferControlBlocks.next;
        } else {
            while(BufferControlBlocks.next != null && BufferControlBlocks.next != ptr) {
                BufferControlBlocks = BufferControlBlocks.next;
            }
            if(BufferControlBlocks.next == null) {
                throw new RuntimeException("未找到指定的BufferControlBlocks");
            }
            BufferControlBlocks.next = ptr.next;
        }
        ptr.next = null;
        if(ptr.dirty == 1) {
            this.dataStorageManager.writePage(pageId,buf[ptr.frameId]);
            this.unSetDirty(ptr.frameId);
        }
    }

    public LRU getLRU(int frameId) {
        LRU p = this.tail;
        while(p != null && p.bcb.frameId != frameId) {
            p = p.pre;
        }
        if(p == null) {
            System.out.println("获取失败：LRU链表中找不到对应的frame");
        }
        return p;
    }

    public void removeLRU(int frameId) {
        if(this.head != null && this.head.bcb.frameId == frameId) {
            this.head = this.head.next;
            this.head.pre = null;
        }
        else if(this.tail != null && this.tail.bcb.frameId == frameId) {
            this.tail = this.tail.pre;
            this.tail.next = null;
        } else {
            LRU p = this.head;
            while(p != null && p.bcb.frameId != frameId) {
                p = p.next;
            }
            if(p == null) {
                System.out.println("删除失败：LRU链表中找不到对应的frame");
            } else {
                p.pre.next = (p.next);
                p.next.pre = p.pre;
            }
        }
    }

    public void setDirty(int frameId) {
        int pid = this.ftop[frameId];
        int fid = this.hash(pid);
        BufferControlBlocks BufferControlBlocks = ptof[fid];
        while(BufferControlBlocks != null && BufferControlBlocks.pageId != pid) {
            BufferControlBlocks = BufferControlBlocks.next;
        }
        if(BufferControlBlocks != null) {
            BufferControlBlocks.dirty = 1;
        }
    }

    public void unSetDirty(int frameId) {
        int pid = this.ftop[frameId];
        int fid = this.hash(pid);
        BufferControlBlocks BufferControlBlocks = ptof[fid];
        while(BufferControlBlocks != null && BufferControlBlocks.pageId != pid) {
            BufferControlBlocks = BufferControlBlocks.next;
        }
        if(BufferControlBlocks != null) {
            BufferControlBlocks.dirty = 0;
        }
    }

    public void writeDirtys() throws IOException {
        int count = 0;
        for(BufferControlBlocks BufferControlBlocks : this.ptof) {
            while(BufferControlBlocks != null) {
                if(BufferControlBlocks.dirty == 1) {
                    count++;
                    dataStorageManager.writePage(BufferControlBlocks.frameId, buf[BufferControlBlocks.frameId]);
                    this.unSetDirty(BufferControlBlocks.frameId);
                }
                BufferControlBlocks = BufferControlBlocks.next;
            }
        }
        System.out.printf("Thread %d - 最后系统结束全部写回了%d块\n" , threadId, count);
    }

    public void printFrame(int frameId) {
        System.out.println(buf[frameId].filed);
    }

}
