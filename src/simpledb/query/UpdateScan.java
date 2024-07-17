package simpledb.query;

import simpledb.record.RID;

public interface UpdateScan extends Scan{
    public void setVal(String fldname, Constant val);

    public void setInt(String fldname, int val);

    public void setString(String fldname, String val);

    public void insert();

    public void delete();

    public RID getRid();

    // Scanを指定のレコードの位置まで移動させる
    public void moveToRid(RID Rid);
}
