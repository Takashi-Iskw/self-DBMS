package simpledb.record;

import simpledb.file.BlockId;
import simpledb.tx.Transaction;
import simpledb.query.*;

import static java.sql.Types.INTEGER;

public class TableScan implements UpdateScan{
    private Transaction tx;
    private Layout layout;
    private RecordPage rp;
    private String filename;
    private int currentslot;

    public TableScan(Transaction tx, String tblname, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        filename = tblname + ".tbl";
        if(tx.size(filename) == 0)
            moveToNewBlock();
        else
            moveToBlock(0);
    }

    // Scanを実装するメソッド群

    public void close() {
        if (rp != null)
            tx.unpin(rp.block());
    }

    public void beforeFirst() {
        moveToBlock(0);
    }

    public boolean next() {
        currentslot = rp.nextAfter(currentslot);        // currentslotの次にUSEDになっているslotを返す。存在しないなら-1
        while(currentslot < 0) {
            if (atLastBlock())
                return false;                           // 見つからないまま最後のブロックに行ったらfalse
            moveToBlock(rp.block().number()+1);
            currentslot = rp.nextAfter(currentslot);    // 現在ブロックでUSEDが見つかればtrue
        }
        return true;                                    // 次のUSEDが見つかればtrue
    }

    public int getInt(String fldname) {
        return rp.getInt(currentslot, fldname);
    }

    public String getString(String fldname) {
        return rp.getString(currentslot, fldname);
    }

    public Constant getVal(String fldname) {
        if(layout.schema().type(fldname) == INTEGER)
            return new IntConstant(getInt(fldname));
        else
            return new StringConstant(getString(fldname));
    }

    // フィールドを持っているか
    public boolean hasField(String fldname) {
        return layout.schema().hasField(fldname);
    }


    // UpdateScanを実装するメソッド群

    public void setInt(String fldname, int val) {
        rp.setInt(currentslot, fldname, val);
    }

    public  void setString(String fldname, String val) {
        rp.setString(currentslot, fldname, val);
    }

    public void setVal(String fldname, Constant val) {
        if (layout.schema().type(fldname) == INTEGER)
            setInt(fldname, (Integer)val.asJavaVal());
        else
            setString(fldname, (String)val.asJavaVal());
    }

    public void insert() {
        currentslot = rp.insertAfter(currentslot);      // EMPTYのslotを探してレコードを挿入
        while(currentslot < 0) {                        // 同じブロック内にEMPTYが見つからなかった場合(-1)
            if(atLastBlock())
                moveToNewBlock();
            else
                moveToBlock(rp.block().number()+1);
            currentslot = rp.insertAfter(currentslot);
        }
    }
    // 何を挿入？
    // → insertAfterでRecordを挿入しようとしている

    public void delete() {
        rp.delete(currentslot);
    }

    public void moveToRid(RID rid) {
        close();
        BlockId blk = new BlockId(filename, rid.blockNumber());
        rp = new RecordPage(tx, blk, layout);
        currentslot = rid.slot();
    }

    public RID getRid() {
        return new RID(rp.block().number(), currentslot);
    }


    // 補助用のprivateメソッド群

    private void moveToBlock(int blknum) {
        close();
        BlockId blk = new BlockId(filename, blknum);
        rp = new RecordPage(tx, blk, layout);
        currentslot = -1;
    }

    private void moveToNewBlock() {
        close();
        BlockId blk = tx.append(filename);
        rp = new RecordPage(tx, blk, layout);
        rp.format();
        currentslot = -1;
    }

    private boolean atLastBlock() {
        return rp.block().number() == tx.size(filename) - 1;
    }
}
