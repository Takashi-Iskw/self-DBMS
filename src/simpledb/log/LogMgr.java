package simpledb.log;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;

import java.util.Iterator;

public class LogMgr {
    private FileMgr fm;
    private String logfile;
    private Page logpage;
    private BlockId currentblk;
    private int latestLSN = 0;
    private int lastSavedLSN = 0;

    public LogMgr(FileMgr fm, String logfile) {
        this.fm = fm;
        this.logfile = logfile;
        byte[] b = new byte[fm.blockSize()];
        logpage = new Page(b);
        int logsize = fm.length(logfile);
        if (logsize == 0) {
            currentblk = appendNewBlock();
        } else {
            currentblk = new BlockId(logfile, logsize - 1);
            fm.read(currentblk, logpage); // バッファ"logPage"にfmの内容を書き込み
        }
    }
    // logPageはここで新規に作成しているようだが、readしても真っ白なのでは？ 初期化みたいなものか
    // ↑違う。逆で、fmの内容をlogpageに書き出している。それがread


    public void flush(int lsn) {
        if (lsn >= lastSavedLSN)
            flush();
    }
    // lastSavedLSNは何

    public Iterator<byte[]> iterator() {
        flush();
        return new LogIterator(fm, currentblk);
    }

    public synchronized int append(byte[] logrec) {
        int boundary = logpage.getInt(0);
        int recsize = logrec.length;
        int bytesneeded = recsize + Integer.BYTES;
        if (boundary - bytesneeded < Integer.BYTES) {
            flush();
            currentblk = appendNewBlock();
            boundary = logpage.getInt(0);
        }
        int recpos = boundary - bytesneeded;
        logpage.setBytes(recpos, logrec);
        logpage.setInt(0, recpos); // 新boundaryをoffset0へ
        latestLSN += 1;
        return latestLSN;
    }

    private BlockId appendNewBlock() {
        BlockId blk = fm.append(logfile);
        logpage.setInt(0, fm.blockSize());
        fm.write(blk, logpage);
        return blk;
    }

    private void flush() {
        fm.write(currentblk, logpage);
        lastSavedLSN = latestLSN;
    }
}
