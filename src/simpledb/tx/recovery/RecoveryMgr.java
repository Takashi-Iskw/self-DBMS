package simpledb.tx.recovery;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;
import simpledb.log.LogMgr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import static simpledb.tx.recovery.LogRecord.*;

public class RecoveryMgr {
    private LogMgr lm;
    private BufferMgr bm;
    private Transaction tx;
    private int txnum;

    public RecoveryMgr(Transaction tx, int txnum, LogMgr lm, BufferMgr bm) {
        this.tx = tx;
        this.txnum = txnum;
        this.lm = lm;
        this.bm = bm;
        StartRecord.writeToLog(lm, txnum);
    }

    public void commit() {
        bm.flushAll(txnum);
        int lsn = CommitRecord.writeToLog(lm, txnum);
        lm.flush(lsn);
    }
    // WALしてなくないか？ 先にデータをflushしている気がする
    // → いや、lm.flushは<Commit>のレコードだけなので、更新やStartなどのログレコードはbm.flushAllで先にWALできているっぽい

    public void rollback() {
        doRollback();
        bm.flushAll(txnum);
        int lsn = RollbackRecord.writeToLog(lm, txnum);
        lm.flush(lsn);
    }

    public void recover() {
        doRecover();
        bm.flushAll(txnum);
        int lsn = CheckpointRecord.writeToLog(lm);
        lm.flush(lsn);
    }

    public int setInt(Buffer buff, int offset, int newval) {
        int oldval = buff.contents().getInt(offset);
        BlockId blk = buff.block();
        return SetIntRecord.writeToLog(lm, txnum, blk, offset, oldval);
    }
    // newvalはどこに行った？ なぜoldvalをログとして書き込むのか

    public int setString(Buffer buff, int offset, String newval) {
        String oldval = buff.contents().getString(offset);
        BlockId blk = buff.block();
        return SetStringRecord.writeToLog(lm, txnum, blk, offset, oldval);
    }

    private void doRollback() {
        Iterator<byte[]> iter = lm.iterator();
        while (iter.hasNext()) {
            byte[] bytes = iter.next();
            LogRecord rec = LogRecord.createLogRecord(bytes);
            if(rec.txNumber() == txnum) {
                if(rec.op() == START)
                    return;                         // 対象TxのSTARTを見つけたらreturnしてロールバック終了
                rec.undo(tx);                       // undoで置き換えるvalをどこから持ってきている？
            }
        }
    }

    private void doRecover() {
        Collection<Integer> finishedTxs = new ArrayList<Integer>();
        Iterator<byte[]> iter = lm.iterator();
        while(iter.hasNext()) {
            byte[] bytes = iter.next();
            LogRecord rec = LogRecord.createLogRecord(bytes);
            if(rec.op() == CHECKPOINT)
                return;
            if(rec.op() == COMMIT || rec.op() == ROLLBACK)
                finishedTxs.add(rec.txNumber());
            else if(!finishedTxs.contains(rec.txNumber()))          // 終了済みTxでなければundo
                                                                    // (コミットを見つける前であれば、後に(それ以前に)コミットするTxもundo)
                rec.undo(tx);
        }
    }
    // Rollbackとイテレータの読み込む方向一緒でいいんだっけ？
}
