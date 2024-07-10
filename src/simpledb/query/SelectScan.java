package simpledb.query;

public class SelectScan implements UpdateScan{
    private Scan s;
    private Predicate pred;

    public SelectScan(Scan s, Predicate pred) {
        this.s = s;
        this.pred = pred;
    }

    // Scan methods

    public void beforeFirst() {
        s.beforeFirst();
    }

    public boolean next() {
        while (s.next())
            if (pred.isSatisfied(s))
                return true;
        return false;
    }

    public int getInt(String fldname) {
        return s.getInt(fldname);
    }

    public String getString(String fldname) {
        return s.getString(fldname);
    }

    public Constant getVal(String fldname) {
        return s.getVal(fldname);
    }

    public boolean hasField(String fldname) {
        return s.hasField(fldname);
    }

    public void close() {
        s.close();
    }

    // UpdateScan methods

    public void setInt(String fldname, int val) {
        UpdateScan us = (UpdateScan) s;
        us.setInt(fldname, val);
    }
}

