package ustc.krazy.structure;

import java.util.Arrays;
import ustc.krazy.constant.BufferConstants;

/**
 * @author krazy
 * @date 2025/1/6
 */
public class bFrame {
    public char[] filed;
    public bFrame() {
        filed = new char[BufferConstants.FRAME_SIZE];
    }
    public bFrame(byte[] buffer) {
        this.filed = new char[BufferConstants.FRAME_SIZE];
        System.arraycopy(Arrays.toString(buffer).toCharArray(),0,this.filed,0, BufferConstants.FRAME_SIZE);
    }
}
