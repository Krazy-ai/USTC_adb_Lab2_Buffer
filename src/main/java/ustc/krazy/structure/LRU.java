package ustc.krazy.structure;

/**
 * @author krazy
 * @date 2025/1/6
 */
public class LRU {
    public BufferControlBlocks bcb;
    public LRU pre;
    public LRU next;

    @Override
    public String toString() {
        return "LRU{" +
                "BufferControlBlocks = " + bcb +
                ", pre = " + pre +
                ", next = " + next +
                '}';
    }
}
