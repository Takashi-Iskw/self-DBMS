package simpledb.metadata;

public class StatInfo {
    private int numBlocks;
    private int numRecs;

    public StatInfo(int numblocks, int numrecs) {
        this.numBlocks = numblocks;
        this.numRecs   = numrecs;
    }

    public int blockAccessed() {
        return numBlocks;
    }

    public int recordsOutput() {
        return numRecs;
    }

    // 着目したフィールドにおける重複の無い値の個数を返す (専攻名や姓名などは他の学生と被る可能性がある)
    public int distinctValues(String fldname) {
        return 1 + (numRecs / 3);               // 1+全レコード数/3 というダイナミックな見積もり
    }
}
