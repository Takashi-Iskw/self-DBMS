package simpledb.tx;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;

import java.util.*;

/**
 * ある一つのTransactionが持つBufferの一覧を提供
 */
public class BufferList {
    private Map<BlockId, Buffer> buffers = new HashMap<>();
    private List<BlockId> pins = new ArrayList<>();
    private BufferMgr bm;

    public BufferList(BufferMgr bm) {
        this.bm = bm;
    }

    /**
     * Buffers(HashMap)から指定ブロックを持つBufferを取得
     * @param blk
     * @return
     */
    Buffer getBuffer(BlockId blk) {
        return buffers.get(blk);
    }

    void pin(BlockId blk) {
        Buffer buff = bm.pin(blk);
        buffers.put(blk, buff);
        pins.add(blk);
    }

    void unpin(BlockId blk) {
        Buffer buff = buffers.get(blk);
        bm.unpin(buff);
        pins.remove(blk);
        if(!pins.contains(blk))
            buffers.remove(blk);
    }
    // !pins.contains(blk) pinsには同じblkが複数入っている場合もあるということ？

    void unpinAll() {
        for(BlockId blk : pins) {
            Buffer buff = buffers.get(blk);
            bm.unpin(buff);
        }
        buffers.clear();
        pins.clear();
    }
}
