package simpledb.tx.concurrency;

import simpledb.file.BlockId;

import java.util.HashMap;
import java.util.Map;

public class LockTable {
    private static final long MAX_TIME = 10000;   // 10秒

    private Map<BlockId, Integer> locks = new HashMap<BlockId, Integer>();

    public synchronized void sLock(BlockId blk) {
        try {
            long timestamp = System.currentTimeMillis();
            while(hasXlock(blk) && !waitingTooLong(timestamp))      // 既に排他ロックされている or 待たされすぎていない
                wait(MAX_TIME);
            if (hasXlock(blk))
                throw new LockAbortException();
            int val = getLockVal(blk);
            locks.put(blk, val+1);
        } catch(InterruptedException e) {
            throw new LockAbortException();
        }
    }

    public synchronized void xLock(BlockId blk) {
        try {
            long timestamp = System.currentTimeMillis();
            while(hasOtherSLocks(blk) && !waitingTooLong(timestamp))
                wait(MAX_TIME);
            if(hasOtherSLocks(blk))
                throw new LockAbortException();
            locks.put(blk, -1);
        } catch(InterruptedException e) {
            throw new LockAbortException();
        }
    }

    public synchronized void unlock(BlockId blk) {
        int val = getLockVal(blk);
        if(val > 1)
            locks.put(blk, val-1);
        else {
            locks.remove(blk);              // 最後のロックの場合、locksから削除
            notifyAll();
        }
    }

    private boolean hasXlock(BlockId blk) {
        return getLockVal(blk) < 0;             // XLock取得時は、locksに1でなく-1を入れるルールになっている。
    }

    private boolean hasOtherSLocks(BlockId blk) {
        return getLockVal(blk) > 1;
    }

    private boolean waitingTooLong(long starttime) {
        return System.currentTimeMillis() - starttime > MAX_TIME;
    }

    private int getLockVal(BlockId blk) {
        Integer ival = locks.get(blk);
        return (ival == null) ? 0 : ival.intValue();
    }
}
