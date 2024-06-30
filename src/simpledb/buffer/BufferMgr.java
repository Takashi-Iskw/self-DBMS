package simpledb.buffer;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;

public class BufferMgr {
    private Buffer[] bufferpool;
    private int numAvailable;
    private static final long MAX_TIME = 10000; // 10秒　

    public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
        bufferpool = new Buffer[numbuffs];
        numAvailable = numbuffs;
        for (int i = 0; i < numbuffs; i++) {
            bufferpool[i] = new Buffer(fm, lm);
        }
    }

    public synchronized int available() {
        return numAvailable;
    }

    public synchronized void flushAll(int txnum) {
        for (Buffer buff : bufferpool)
            if (buff.modifyingTx() == txnum)
                buff.flush();
    }
    // 書き換え中のTxのみをflush? modifyingTxが分からない

    public synchronized void unpin(Buffer buff) {
        buff.unpin();
        if(!buff.isPinned()) {  // pins > 0かどうか
            numAvailable++;
            notifyAll();
        }
    }
    // 普通はunpinしたらbuffのpinは0にならないのか？ → いや、同じblkに複数buffがピンされると2以上になる
    // notifyAll分からん

    public synchronized Buffer pin(BlockId blk) {
        try {
            long timestamp = System.currentTimeMillis();
            Buffer buff = tryToPin(blk);                         // 既にピンされていないか、空いているバッファはないか
            while(buff == null && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);
                buff = tryToPin(blk);
            }
            if(buff == null) {                                   // 充分待ってもpinできなかった場合
                throw new BufferAbortException();
            }
            return buff;
        } catch (InterruptedException e) {
            throw new BufferAbortException();
        }
    }

    private boolean waitingTooLong(long starttime) {
        return System.currentTimeMillis() - starttime > MAX_TIME;
    }

    private Buffer tryToPin(BlockId blk) {
        Buffer buff = findExistingBuffer(blk); // 既に対象ブロックが別のバッファでピンされていないか探す
        if(buff == null) {                     // ピンされていない場合
            buff = chooseUnpinnedBuffer();     // プール内で空いてるバッファを探す
            if(buff == null) {
                return null;
            }
            buff.assignToBlock(blk);           // 空いてるバッファに割り当て
        }
        if (!buff.isPinned())                  // 普通は実行されそう？
            numAvailable--;                    // → いや、existingの場合は次の行でpin++する前からisPinnedされているので、numAvailableが減らない
        buff.pin();                            // existingでもpin++される(pinが2以上になる)
        return buff;
    }

    // 引数のブロックと同一のブロックをピンしているバッファを返す
    private Buffer findExistingBuffer(BlockId blk) {
        for(Buffer buff : bufferpool) {
            BlockId b = buff.block();
            if (b != null && b.equals(blk))
                return buff;
        }
        return null;
    }

    // 空いてるバッファを返す
    private Buffer chooseUnpinnedBuffer() {
        for(Buffer buff : bufferpool) {
            if (!buff.isPinned())
                return buff;
        }
        return null;
    }
}
