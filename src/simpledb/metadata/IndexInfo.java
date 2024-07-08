package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import static java.sql.Types.INTEGER;

// ある一つのIndexの情報を取り扱う
public class IndexInfo {
    private String idxname, fldname;
    private Transaction tx;
    private Schema tblSchema;
    private Layout idxLayout;
    private StatInfo si;

    public IndexInfo(String idxname, String fldname, Schema tblSchema, Transaction tx, StatInfo si) {
        this.idxname = idxname;
        this.fldname = fldname;
        this.tx = tx;
        this.tblSchema = tblSchema;
        this.idxLayout = createIdxLayout();
        this.si = si;
    }

    public Index open() {
//        Schema sch = Schema();
        return new HashIndex(tx, idxname, idxLayout);
//        return new BTreeIndex(tx, idxname, idxLayout);
    }

    public int blocksAccessed() {
        int rpb = tx.blockSize() / idxLayout.slotSize();        // Record Per Block
        int numblocks = si.recordsOutput() / rpb;
        return HashIndex.searchCost(numblocks, rpb);
//        return BTreeIndex.searchCost(numblocks, rpb);
    }

    public int recordsOutput() {
        return si.recordsOutput() / si.distinctValues(fldname);     // si.recordsOutput() -> レコードの数
    }

    public int distinctValues(String fname) {
        return fldname.equals(fname) ? 1 : si.distinctValues(fldname);
    }

    private Layout createIdxLayout() {
        Schema sch = new Schema();
        sch.addIntField("block");
        sch.addIntField("id");
        if(tblSchema.type(fldname) == INTEGER)
            sch.addIntField("dataval");
        else {
            int fldlen = tblSchema.length(fldname);
            sch.addStringField("dataval", fldlen);
        }
        return new Layout(sch);
    }
    // idは何？
}
