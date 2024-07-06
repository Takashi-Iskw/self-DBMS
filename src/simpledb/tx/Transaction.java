package simpledb.tx;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;
import simpledb.log.LogMgr;
import simpledb.tx.concurrency.ConcurrencyMgr;
import simpledb.tx.recovery.RecoveryMgr;

public class Transaction {
    private static int nextTxNum = 0;
    private static final int END_OF_FILE = -1;
    private RecoveryMgr recoveryMgr;
    private ConcurrencyMgr concurMgr;
    private BufferMgr bm;
    private FileMgr fm;
    private int txnum;
    private BufferList mybuffers;

    public Transaction(FileMgr fm, LogMgr lm, BufferMgr bm) {
        this.fm = fm;
        this.bm = bm;
        txnum = nextTxNumber();
        recoveryMgr = new RecoveryMgr(this, txnum, lm, bm);          //  TxとRecMgr 1v1対応
        concurMgr = new ConcurrencyMgr();                               //  TxとConMgr 1v1対応 (LockTableは共有)
        mybuffers = new BufferList(bm);
    }

    public void commit() {
        recoveryMgr.commit();
        concurMgr.release();
        mybuffers.unpinAll();
        System.out.println("transaction " + txnum + " commited");
    }

    public void rollback() {
        recoveryMgr.rollback();
        concurMgr.release();
        mybuffers.unpinAll();
        System.out.println("transaction " + txnum + " rolled back");
    }

    public void recover() {
        bm.flushAll(txnum);
        recoveryMgr.recover();
    }
    // 一回flushAllする理由は？

    public void pin(BlockId blk) {
        mybuffers.pin(blk);
    }

    public void unpin(BlockId blk) {
        mybuffers.unpin(blk);
    }

    public int getInt(BlockId blk, int offset) {
        concurMgr.sLock(blk);
        Buffer buff = mybuffers.getBuffer(blk);
        return buff.contents().getInt(offset);
    }

    public String getString(BlockId blk, int offset) {
        concurMgr.sLock(blk);
        Buffer buff = mybuffers.getBuffer(blk);
        return buff.contents().getString(offset);
    }

    public void setInt(BlockId blk, int offset, int val, boolean okToLog) {
        concurMgr.xLock(blk);
        Buffer buff = mybuffers.getBuffer(blk);
        int lsn = -1;
        if (okToLog)
            lsn = recoveryMgr.setInt(buff, offset, val);
        Page p = buff.contents();
        p.setInt(offset, val);
        buff.setModified(txnum, lsn);
    }
    // recoveryMgrのsetIntとは別に、PageのsetIntをvalを用いて実行している

    public void setString(BlockId blk, int offset, String val, boolean okToLog) {
        concurMgr.xLock(blk);
        Buffer buff = mybuffers.getBuffer(blk);
        int lsn = -1;
        if (okToLog)
            lsn = recoveryMgr.setString(buff, offset, val);
        Page p = buff.contents();
        p.setString(offset, val);
        buff.setModified(txnum, lsn);
    }

    public int size(String filename) {
        BlockId dummyblk = new BlockId(filename, END_OF_FILE);      // END_OF_FILE = -1
        concurMgr.sLock(dummyblk);
        return fm.length(filename);
    }

    public BlockId append(String filename) {
        BlockId dummyblk = new BlockId(filename, END_OF_FILE);
        concurMgr.xLock(dummyblk);
        return fm.append(filename);
    }

    public int blockSize() {
        return fm.blockSize();
    }

    public int availableBuffs() {
        return bm.available();
    }

    public static synchronized int nextTxNumber() {
        nextTxNum++;
        System.out.println("new transaction: " + nextTxNum);
        return nextTxNum;
    }
}
