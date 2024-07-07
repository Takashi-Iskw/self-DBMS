package simpledb.record;

import java.util.*;
import static java.sql.Types.*;

public class Schema {
    private List<String> fields = new ArrayList<>();
    private Map<String, FieldInfo> info = new HashMap<>();

    public void addField(String fldname, int type, int length) {
        fields.add(fldname);
        info.put(fldname, new FieldInfo(type, length));
    }
    // typeとは？ (おそらくINTEGERとか)
    // 固定長のフィールドだけ？

    public void addIntField(String fldname) {
        addField(fldname, INTEGER, 0);
    }

    public void addStringField(String fldname, int length) {
        addField(fldname, VARCHAR, length);
    }

    public void add(String fldname, Schema sch) {
        int type = sch.type(fldname);
        int length = sch.length(fldname);
        addField(fldname, type, length);
    }

    public void addAll(Schema sch) {
        for(String fldname : sch.fields())
            add(fldname, sch);
    }

    public List<String> fields() {
        return fields;
    }

    public boolean hasField(String fldname) {
        return fields.contains(fldname);
    }

    public int type(String fldname) {
        return info.get(fldname).type;
    }

    public int length(String fldname) {
        return info.get(fldname).length;
    }

    class FieldInfo {
        int type, length;
        public FieldInfo(int type, int length) {
            this.type = type;
            this.length = length;
        }
    }
}
