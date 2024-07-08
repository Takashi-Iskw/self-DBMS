package simpledb.record;

import simpledb.file.BlockId;
import simpledb.tx.Transaction;

import static java.sql.Types.INTEGER;

public class RecordPage {
    public static final int EMPTY = 0, USED = 1;
    private Transaction tx;
    private BlockId blk;
    private Layout layout;

    public RecordPage(Transaction tx, BlockId blk, Layout layout) {
        this.tx = tx;
        this.blk = blk;
        this.layout = layout;
        tx.pin(blk);
    }
    // 一つのブロックにつき一種類のLayoutのみしか記載できない？
    // tx.pinは色んなところで出てくる気がするが、同一txや同一buffが同じblkを多重でpinしたりしないのだろうか？

    public int getInt(int slot, String fldname) {
        int fldpos = offset(slot) + layout.offset(fldname);
        return tx.getInt(blk, fldpos);
    }

    public String getString(int slot, String fldname) {
        int fldpos = offset(slot) + layout.offset(fldname);
        return tx.getString(blk, fldpos);
    }

    public void setInt(int slot, String fldname, int val) {
        int fldpos = offset(slot) + layout.offset(fldname);
        tx.setInt(blk, fldpos, val, true);
    }

    public void setString(int slot, String fldname, String val) {
        int fldpos = offset(slot) + layout.offset(fldname);
        tx.setString(blk, fldpos, val, true);
    }

    // slot個目のレコードのフラグを0に
    public void delete(int slot) {
        setFlag(slot, EMPTY);
    }

    // blk内の全てのレコードを初期化するイメージ (フラグを未使用に、整数フィールドを0に、文字列フィールドを""に)
    public void format() {
        int slot = 0;
        while(isValidSlot(slot)) {
            tx.setInt(blk, offset(slot), EMPTY, false);
            Schema sch = layout.schema();
            for(String fldname : sch.fields()) {
                int fldpos = offset(slot) + layout.offset(fldname);
                if(sch.type(fldname) == INTEGER)
                    tx.setInt(blk, fldpos, 0, false);
                else tx.setString(blk, fldpos, "", false);
            }
            slot++;
        }
    }

    public int nextAfter(int slot) {
        return searchAfter(slot, USED);
    }

    public int insertAfter(int slot) {
        int newslot = searchAfter(slot, EMPTY);
        if(newslot >= 0)                            // ブロック内に収まらない場合はsearchAfterで-1が返されてしまう
            setFlag(newslot, USED);
        return newslot;
    }

    public BlockId block() {
        return blk;
    }


    // 以下補助用のprivateメソッド

    private void setFlag(int slot, int flag) {
        tx.setInt(blk, offset(slot), flag, true);
    }

    private int searchAfter(int slot, int flag) {
        slot++;
        while(isValidSlot(slot)) {
            if (tx.getInt(blk, offset(slot)) == flag)
                return slot;
            slot++;
        }
        return -1;
    }
    // 使用中/未使用 flag を指定し、slot番目以降の、そのflagのスロットを検索？

    // 次のslotがブロックに収まるかどうか
    private boolean isValidSlot(int slot) {
        return offset(slot+1) <= tx.blockSize();
    }

    // ブロック内でslot番目のレコードのオフセットを返す
    private int offset(int slot) {
        return slot * layout.slotSize();
    }
}
