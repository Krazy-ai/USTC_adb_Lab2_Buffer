package ustc.krazy.structure;

/**
 * @author krazy
 * @date 2025/1/6
 *  缓冲区控制块
 */
public class BufferControlBlocks {
    public int pageId;
    public int frameId;
    public int count;
    public int dirty;
    public int latch;
    public BufferControlBlocks next;

    @Override
    public String toString() {
        return "BCB{" +
                "pageId=" + pageId +
                ", frameId=" + frameId +
                ", count=" + count +
                ", dirty=" + dirty +
                ", next=" + next +
                '}';
    }
}
