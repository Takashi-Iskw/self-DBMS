package simpledb.query;

public class Constant implements Comparable<Constant>{
    private Integer ival = null;
    private String sval = null;

    public Constant(Integer ival) {
        this.ival = ival;
    }

    public Constant(String sval) {
        this.sval = sval;
    }

    public int asInt() {
        return ival;
    }

    public String asString() {
        return sval;
    }

    public boolean equals(Object obj) {
        Constant c = (Constant) obj;
        return (ival != null) ? ival.equals(c.ival)         // 整数型か文字列型か
                              : sval.equals(c.sval);
    }

    public int compareTo(Constant c) {
        return (ival != null) ? ival.compareTo(c.ival)         // 整数型か文字列型か
                              : sval.compareTo(c.sval);
    }

    public int hashCode() {
        return (ival != null) ? ival.hashCode() : sval.hashCode();
    }

    public String toString() {
        return (ival != null) ? ival.toString() : sval.toString();
    }
}
