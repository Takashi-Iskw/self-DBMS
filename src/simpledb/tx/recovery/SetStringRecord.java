package simpledb.tx.recovery;

import simpledb.file.*;
import simpledb.log.LogMgr;

public class SetStringRecord implements LogRecord {
    private int txnum, offset;
    private String val;
    private BlockId blk;

    public SetStringRecord(Page p) {
        int tpos = Integer.BYTES;
        txnum = p.getInt(tpos);
        int fpos = tpos + Integer.BYTES;
        String filename = p.getString(fpos);
        int bpos = fpos + Page.maxLength(filename.length());
        int blknum = p.getInt(bpos);
        blk = new BlockId(filename, blknum);
        int opos = bpos + Integer.BYTES;
        offset = p.getInt(opos);
        int vpos = opos + Integer.BYTES;
        val = p.getString(vpos);
    }
    // 既存のログレコードから値を読み取っているような印象

    public int op() {
        return SETSTRING;
    }

    public int txNumber() {
        return txnum;
    }

    public String toString() {
        return "<SETSTRING " + txnum + " " + blk + " " + offset + " " + val + ">";
    }

    public void undo(Transaction tx) {
        tx.pin(blk);
        tx.setString(blk, offset, val, false);    // undoはログとして記録しない
        tx.unpin(blk);
    }
    // undoなのにsetしている？ valはなんだ

    public static int writeToLog(LogMgr lm, int txnum, BlockId blk, int offset, String val) {
        int tpos = Integer.BYTES;
        int fpos = tpos + Integer.BYTES;
        int bpos = fpos + Page.maxLength(blk.fileName().length());
        int opos = bpos + Integer.BYTES;
        int vpos = opos + Integer.BYTES;
        int reclen = vpos + Page.maxLength(val.length());       // レコード長さ
        byte[] rec = new byte[reclen];
        Page p = new Page(rec);
        p.setInt(0, SETSTRING);
        p.setInt(tpos, txnum);
        p.setString(fpos, blk.fileName());
        p.setInt(bpos, blk.number());
        p.setInt(opos, offset);
        p.setString(vpos, val);
        return lm.append(rec);                                  // appendはlsnがreturnされる！
    }
    // staticなのでインスタンス化しなくても呼び出せる(値さえ入れればいきなりログレコードを作れる)
    // レコードの作成とログへのappendを同時に行っている
}
