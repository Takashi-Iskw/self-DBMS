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
        pins.remove(blk);                   // pinは一つしか削除されない
        if(!pins.contains(blk))
            buffers.remove(blk);
    }
    // !pins.contains(blk) pinsには同じblkが複数入っている場合もあるということ？
    // tx.pinをしたあと、RecordPageのコンストラクタでtx.pinをした場合などが該当しそう

    void unpinAll() {
        for(BlockId blk : pins) {                       // 同じbuffに対する、同じblkに重複して取ったpinも全部削除
            Buffer buff = buffers.get(blk);
            bm.unpin(buff);
        }
        buffers.clear();
        pins.clear();
    }
}
