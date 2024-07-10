package simpledb.query;

public class ProductScan implements Scan{
    private Scan s1, s2;

    public ProductScan(Scan s1, Scan s2) {
        this.s1 = s1;
        this.s2 = s2;
        s1.next();
    }

    public void beforeFirst() {
        s1.beforeFirst();
        s1.next();
        s2.beforeFirst();
    }

    // s1のループの中にs2のループがあるイメージ。s1を1個進めてs2全体を進める、を繰り返す。
    public boolean next() {
        if(s2.next())
            return true;
        else {                                  // s2のループが最後まで進んだ場合
            s2.beforeFirst();
            return s2.next() && s1.next();
        }
    }

    public int getInt(String fldname) {
        if (s1.hasField(fldname))
            return s1.getInt(fldname);
        else
            return s2.getInt(fldname);
    }

    public String getString(String fldname) {
        if (s1.hasField(fldname))
            return s1.getString(fldname);
        else
            return s2.getString(fldname);
    }

    public Constant getVal(String fldname) {
        if (s1.hasField(fldname))
            return s1.getVal(fldname);
        else
            return s2.getVal(fldname);
    }

    public boolean hasField(String fldname) {
        return s1.hasField(fldname) || s2.hasField(fldname);
    }

    public void close() {
        s1.close();
        s2.close();
    }
}
