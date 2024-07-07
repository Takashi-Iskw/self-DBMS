package simpledb.record;

import simpledb.file.Page;

import java.util.*;

import static java.sql.Types.INTEGER;

public class Layout {
    private Schema schema;                              // LayoutがSchemaを保有。Schemaの形状のレイアウトを示すものがLayout？
    private Map<String, Integer> offsets;
    private int slotsize;

    public Layout(Schema schema) {
        this.schema = schema;
        offsets = new HashMap<>();
        int pos = Integer.BYTES;                          // 使用中/未使用 のフラグ用スペース
        for (String fldname : schema.fields()) {          // Schemaは整数とか文字列とか色んなフィールドを持つ(科目コード、科目名とか)
            offsets.put(fldname, pos);
            pos += lengthInBytes(fldname);
        }

        slotsize = pos;
    }
    // offsetsは何のオフセット？
    // → フィールド名の列挙のHashMapにおける、各フィールドの先頭位置のオフセット
    // slotsizeは何？ Schema全体の長さみたいな？

    public Layout(Schema schema, Map<String, Integer> offsets, int slotsize) {
        this.schema = schema;
        this.offsets = offsets;
        this.slotsize = slotsize;
    }

    public Schema schema() {
        return schema;
    }

    public int offset(String fldname) {
        return offsets.get(fldname);
    }

    public int slotSize() {
        return slotsize;
    }

    private int lengthInBytes(String fldname) {
        int fldtype = schema.type(fldname);
        if(fldtype == INTEGER)
            return Integer.BYTES;
        else                                                        // VARCHAR
            return Page.maxLength(schema.length(fldname));          // 先頭の文字列長さを含んだ文字数
    }
}
