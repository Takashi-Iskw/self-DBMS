package simpledb.tx.concurrency;

import simpledb.file.BlockId;

import java.util.HashMap;
import java.util.Map;

public class ConcurrencyMgr {
    private static LockTable locktbl = new LockTable();
    private Map<BlockId, String> locks = new HashMap<BlockId, String>();

    public void sLock(BlockId blk) {
        if(locks.get(blk) == null) {            // 対象blkが S にも X にもなってないならsLockを取得
            locktbl.sLock(blk);
            locks.put(blk, "S");
        }
    }
    // 一回チャレンジしてダメだったら待つ、ということについて
    // LockTableの方にwaitの処理があったが、ここでifの内側にあるのでは意味ないのでは？

    public void xLock(BlockId blk) {
        if(!hasXLock(blk)) {
            sLock(blk);
            locktbl.xLock(blk);
            locks.put(blk, "X");
        }
    }

    public void release() {
        for(BlockId blk : locks.keySet())
            locktbl.unlock(blk);                    // あるTxが持つlockの個数-- lockが0になるならMapから削除
        locks.clear();                              // あるTxが持つlocks内の全てのエントリーを削除
    }

    private boolean hasXLock(BlockId blk) {
        String locktype = locks.get(blk);
        return locktype != null && locktype.equals("X");
    }
}
