package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

import java.util.HashMap;
import java.util.Map;

public class TableMgr {
    public static final int MAX_NAME = 16;      // テーブル名、フィールド名の最大長さ
    private Layout tcatLayout, fcatLayout;

    public TableMgr(boolean isNew, Transaction tx) {
        Schema tcatSchema = new Schema();
        tcatSchema.addStringField("tblname", MAX_NAME);
        tcatSchema.addIntField("slotsize");
        tcatLayout = new Layout(tcatSchema);
        Schema fcatSchema = new Schema();
        fcatSchema.addStringField("tblname", MAX_NAME);
        fcatSchema.addStringField("fldname", MAX_NAME);
        fcatSchema.addIntField("type");
        fcatSchema.addIntField("length");
        fcatSchema.addIntField("offset");
        fcatLayout = new Layout(fcatSchema);
        if(isNew) {
            createTable("tblcat", tcatSchema, tx);
            createTable("fldcat", fcatSchema, tx);
        }
    }

    // Schemaの情報をもとに、tblnameというテーブルを作成
    public void createTable(String tblname, Schema sch, Transaction tx) {
        Layout layout = new Layout(sch);

        // tblcatに、対象テーブルの情報に関するレコードを挿入
        TableScan tcat = new TableScan(tx, "tblcat", tcatLayout);
        tcat.insert();
        tcat.setString("tblname", tblname);
        tcat.setInt("slotsize", layout.slotSize());
        tcat.close();

        // fldcatに、対象テーブルのフィールドに関するレコードを挿入
        TableScan fcat = new TableScan(tx, "fldcat", fcatLayout);
        for(String fldname : sch.fields()) {
            fcat.insert();
            fcat.setString("tblname", tblname);
            fcat.setString("fldname", fldname);
            fcat.setInt   ("type", sch.type(fldname));          // INTEGERなら4, VARCHARなら12 (JDBCで定義されている)
            fcat.setInt   ("length", sch.length(fldname));      // INTEGERなら4バイト、VARCHAR(String)なら文字数を示す整数の4バイト+文字数分のバイト
            fcat.setInt   ("offset", layout.offset(fldname));   // そのフィールドの開始位置
        }
        fcat.close();
    }

    // テーブルカタログとフィールドカタログから既存のテーブルを検索し、Layoutを作成して返す。
    // Schemaは自分で作るがLayoutは自分で作るものではなさそう
    public Layout getLayout(String tblname, Transaction tx) {
        int size = -1;
        TableScan tcat = new TableScan(tx, "tblcat", tcatLayout);
        while(tcat.next())                // tcat.currentslotを更新しているので、カーソルが変わっているイメージ
            if(tcat.getString("tblname").equals(tblname)) {
                size = tcat.getInt("slotsize");
                break;
            }
        tcat.close();
        Schema sch = new Schema();
        Map<String, Integer> offsets = new HashMap<String, Integer>();
        TableScan fcat = new TableScan(tx, "fldcat", fcatLayout);
        while(fcat.next())
            if(fcat.getString("tblname").equals(tblname)) {
                String fldname = fcat.getString("fldname");
                int fldtype = fcat.getInt("type");
                int fldlen  = fcat.getInt("length");
                int offset  = fcat.getInt("offset");
                offsets.put(fldname, offset);
                sch.addField(fldname, fldtype, fldlen);
            }
        fcat.close();
        return new Layout(sch, offsets, size);
    }
}
