package simpledb.tx.recovery;

import simpledb.file.Page;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

public class StartRecord implements LogRecord {
   private int txnum;

   public StartRecord(Page p) {
      int tpos = Integer.BYTES;
      txnum = p.getInt(tpos);
   }
   
   public int op() {
      return START;
   }
   
   public int txNumber() {
      return txnum;
   }

   public void undo(Transaction tx) {}
   
   public String toString() {
      return "<START " + txnum + ">";
   }

   public static int writeToLog(LogMgr lm, int txnum) {
      byte[] rec = new byte[2*Integer.BYTES];
      Page p = new Page(rec);
      p.setInt(0, START);
      p.setInt(Integer.BYTES, txnum);
      return lm.append(rec);
   }
}
