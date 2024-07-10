package simpledb.query;

import simpledb.record.Schema;

public class Expression {
    private Constant val = null;
    private String fldname = null;

    // Expressionが定数(int, String)の場合
    public Expression(Constant val) {
        this.val = val;
    }

    // Expressionがフィールド名の場合
    public Expression(String fldname) {
        this.fldname = fldname;
    }

    // Expressionがフィールド名を示すかどうか
    public  boolean isFieldName() {
        return fldname != null;
    }

    public Constant asConstant() {
        return val;
    }

    public String asFieldName() {
        return fldname;
    }

    // 現在のスキャンのレコードに対するExpressionの値を返す
    public Constant evaluate(Scan s) {
        return (val != null) ? val : s.getVal(fldname);     // 定数ならvalをそのまま返す
    }

    // plannerがExpressionのスコープを決めるために使用
    public boolean appliesTo(Schema sch) {
        return (val != null) ? true : sch.hasField(fldname);
    }
    // Expressionが定数なら使えるし、フィールド名ならそのフィールドが存在すれば使える

    public String toString() {
        return (vaal != null) ? val.toString() : fldname;
    }
}
