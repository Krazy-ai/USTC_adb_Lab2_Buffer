package ustc.krazy.manager;

import ustc.krazy.structure.BufferControlBlocks;
import ustc.krazy.structure.LRU;
import ustc.krazy.structure.bFrame;

import java.io.IOException;

import static ustc.krazy.constant.BufferConstants.*;

/**
 * @author krazy
 * @date 2025/1/6
 */
public class BufferManager {
    // 两个哈希表
    private final int[] ftop = new int[DEF_BUFFER_SIZE]; // frameId -> pageId
    private final BufferControlBlocks[] ptof = new BufferControlBlocks[DEF_BUFFER_SIZE]; // pageId -> frameId

    // 缓冲区结构
    public final bFrame[] buf = new bFrame[DEF_BUFFER_SIZE];
    // 数据存储管理器
    private final DataStorageManager dataStorageManager = new DataStorageManager();

    // LRU链表的头和尾部
    private LRU head;
    private LRU tail;
    public static int hitCounter = 0;

    public BufferManager(){
        for(int i = 0;i < DEF_BUFFER_SIZE;i++) {
            this.ptof[i] = null;
            this.ftop[i] = -1;
        }
        int success = this.dataStorageManager.openFile("data.dbf");
        if(success == 0) {
            System.out.println("打开文件异常");
        }
    }

    @Override
    protected void finalize() throws Throwable {
        dataStorageManager.closeFile();
    }

    /**
     *
     * @param pageId
     * @param prot
     * @return
     * @description 查看页面是否已经在缓冲区中，如果是，
     * 则返回相应的frame_id。如果该页面还没有驻留在缓冲区中，
     * 则它会根据需要选择一个Victim Page，并加载到请求的页面中。
     */
    public int fixPage(int pageId, int prot) {
        BufferControlBlocks BufferControlBlocks = this.ptof[this.hash(pageId)];
        while(BufferControlBlocks != null && BufferControlBlocks.pageId != pageId) {
            BufferControlBlocks = BufferControlBlocks.next;
        }
        if(BufferControlBlocks != null) {
            // 页面在缓冲区中
            hitCounter++;
            LRU p = this.getLRUEle(BufferControlBlocks.frameId);
            if(p == null) {
                throw new RuntimeException("buffer命中了，但是LRU链表中找不到对应的结点");
            } else{
                // 只有p不在表尾时才要调整
                if(p.next != null) {
                    if(p.pre == null) {
                        // 在开头，特殊处理
                        this.head = p.next;
                        this.head.pre = null;
                        p.next = null;
                        this.tail.next = p;
                        p.pre = this.tail;
                        p.next = null;
                        this.tail = p;
                    } else {
                        // 1.删除该结点
                        p.pre.next = p.next;
                        p.next.pre = p.pre;

                        // 2.将该节点放到表尾
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
        // 缓存未命中
        int victimFrameId = this.selectVictim();
        BufferControlBlocks nowBufferControlBlocks = new BufferControlBlocks();
        nowBufferControlBlocks.pageId = pageId;
        nowBufferControlBlocks.frameId = victimFrameId;
        nowBufferControlBlocks.count++;

        if(ftop[victimFrameId] != -1) {
            // 如果这个frame已经被使用了，则先移除这个frame
            BufferControlBlocks victimBufferControlBlocks = ptof[hash(ftop[victimFrameId])];
            while(victimBufferControlBlocks != null && victimBufferControlBlocks.frameId != victimFrameId) {
                victimBufferControlBlocks = victimBufferControlBlocks.next;
            }
            if(victimBufferControlBlocks == null) {
                throw new RuntimeException("selectVictim未找到对应的页帧");
            }
            // System.out.println(victimBufferControlBlocks);
            // 移除LRU链表中的该元素，并且修改hash表
            this.removeBufferControlBlocks(victimBufferControlBlocks,victimBufferControlBlocks.pageId);
            this.ftop[victimBufferControlBlocks.frameId] = -1;
            this.removeLRUEle(victimBufferControlBlocks.frameId);
        }

        // 给新调入的页面分配BufferControlBlocks,并且修改哈希表以及LRU链表
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
//            System.out.println(head);
//            System.out.println(tail);
        } else {
            this.tail.next = node;
            node.pre = this.tail;
            node.next = null;
            this.tail = node;
        }


//        if(x == 2) {
//            System.out.println(head);
//            System.out.println(tail);
//            System.exit(0);
//        }
        // 最后读/写入调入的页面
        try {
            if(prot == 0) {
                this.buf[nowBufferControlBlocks.frameId] = dataStorageManager.readPage(nowBufferControlBlocks.pageId);
            } else {
                this.buf[nowBufferControlBlocks.frameId] = new bFrame(new byte[FRAME_SIZE]);
            }
        } catch (IOException e) {
            throw new RuntimeException("读入调入页面异常");
        }
        return nowBufferControlBlocks.frameId;
    }

    public int fixNewPage() {
        int[] pages = dataStorageManager.getPages();
        if(dataStorageManager.getNumPages() == pages.length) {
            return -1;
        }
        for(int pageId = 0; pageId < pages.length; pageId++) {
            if(pages[pageId] == 0) {
                dataStorageManager.setUse(pageId,MAX_PAGES);
                dataStorageManager.incNumPages();
                fixPage(pageId,0);
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
            return -1;
        }
        else {
            BufferControlBlocks.count --;
            return BufferControlBlocks.frameId;
        }
    }

    public int numFreeFrames() { // 返回第一个可用的frameId
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
//        x++;
//        System.out.println(x);
        if(this.numFreeFrames() != -1) {
            return this.numFreeFrames();
        } else {
            LRU p = this.head;
            // System.out.println(p);
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
//        System.out.println("ptr:" + ptr);
//        System.out.println(pageId);
        BufferControlBlocks BufferControlBlocks = this.ptof[this.hash(pageId)];
        // System.out.println("1:" + BufferControlBlocks);
        if(BufferControlBlocks == null) {
            return;
        }
        if(BufferControlBlocks == ptr) {
            this.ptof[this.hash(pageId)] = BufferControlBlocks.next;
        } else {
            while(BufferControlBlocks.next != null && BufferControlBlocks.next != ptr) {
                // System.out.println(BufferControlBlocks);
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

    public LRU getLRUEle(int frameId) {
        LRU p = this.tail;
        while(p != null && p.bcb.frameId != frameId) {
            p = p.pre;
        }
        if(p == null) {
            System.out.println("获取失败：LRU链表中找不到对应的frame");
        }
        return p;
    }

    public void removeLRUEle(int frameId) {
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
        System.out.println("最后系统结束全部写回了" + count + "块");
    }

    public void printFrame(int frameId) {
        System.out.println(buf[frameId].filed);
    }

}
