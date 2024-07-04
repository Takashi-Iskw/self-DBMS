package simpledb.tx.recovery;

import simpledb.file.Page;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

public class CheckpointRecord implements LogRecord {
   public CheckpointRecord() {
   }

   public int op() {
      return CHECKPOINT;
   }

   public int txNumber() {
      return -1; // dummy value
   }
   // Checkpointは特定のTxnumを持たないので、ダミーの-1を返す

   public void undo(Transaction tx) {}

   public String toString() {
      return "<CHECKPOINT>";
   }

   public static int writeToLog(LogMgr lm) {
      byte[] rec = new byte[Integer.BYTES];
      Page p = new Page(rec);
      p.setInt(0, CHECKPOINT);
      return lm.append(rec);
   }
}
