package simpledb.query;

import simpledb.record.Schema;

public class Term {
    private Expression lhs, rhs;

    public Term(Expression lhs, Expression rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    public boolean isSatisfied(Scan s) {
        Constant lhsval = lhs.evaluate(s);
        Constant rhsval = rhs.evaluate(s);
        return rhsval.equals(lhsval);
    }

    public boolean appliesTo(Schema sch) {
        return lhs.appliesTo(sch) && rhs.appliesTo(sch);
    }

    public int reductionFactor(Plan p) {
        String lhsName, rhsName;
        if (lhs.isFieldName() && rhs.isFieldName()) {           // 両方フィールド名
            lhsName = lhs.asFieldName();
            rhsName = rhs.asFieldName();
            return Math.max(p.distinctValues(lhsName),
                            p.distinctValues(rhsName));
        }
        if (lhs.isFieldName()) {                                // 左だけフィールド名
            lhsName = lhs.asFieldName();
            return p.distinctValues(lhsName);
        }
        if (rhs.isFieldName()) {                                // 右だけフィールド名
            rhsName = rhs.asFieldName();
            return p.distinctValues(rhsName);
        }
        // distinctValuesが分からん


        if (lhs.asConstant().equals(rhs.asConstant()))          // 両方が定数
            return 1;
        else
            return Integer.MAX_VALUE;
    }

    public Constant equatesWithConstant(String fldname) {
        if     (lhs.isFieldName() &&
                lhs.asFieldName().equals(fldname) &&
                !rhs.isFieldName())
            return rhs.asConstant();
        else if (rhs.isFieldName() &&
                 rhs.asFieldName().equals(fldname) &&
                 !lhs.isFieldName())
            return lhs.asConstant();
        else
            return null;
    }
    // プランナーがindex作成をいつ使用するかを決定するのに役立つ

    public String equatesWithField(String fldname) {
        if     (lhs.isFieldName() &&
                lhs.asFieldName().equals(fldname) &&
                rhs.isFieldName())
            return rhs.asFieldName();
        else if (rhs.isFieldName() &&
                 rhs.asFieldName().equals(fldname) &&
                 lhs.isFieldName())
            return lhs.asFieldName();
        else
            return null;
    }

    public String toString() {
        return lhs.toString() + "=" + rhs.toString();
    }
}
