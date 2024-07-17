package simpledb.parse;

import simpledb.query.Constant;
import simpledb.query.Expression;
import simpledb.query.Predicate;
import simpledb.query.Term;
import simpledb.record.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class Parser {
    private Lexer lex;

    public Parser(String s) {
        lex = new Lexer(s);
    }
    // あるクエリを与えられたパーサーがレキサーを持つ

    // 述語とその要素を解析するメソッド群

    public String field() {
        return lex.eatId();
    }

    public Constant constant() {
        if (lex.matchStringConstant())
            return new Constant(lex.eatStringConstant());
        else
            return new Constant(lex.eatIntConstant());
    }

    public Expression expression() {
        if (lex.matchId())
            return new Expression(field());
        else
            return new Expression(constant());
    }

    public Term term() {
        Expression lhs = expression();
        lex.eatDelim('=');
        Expression rhs = expression();
        return new Term(lhs, rhs);
    }

    public Predicate predicate() {
        Predicate pred = new Predicate(term());
        if (lex.matchKeyword("and")) {
            lex.eatKeyword("and");
            pred.conjoinWith(predicate());
        }
        return pred;
    }

    // クエリの解析のメソッド群

    public QueryData query() {
        lex.eatKeyword("select");
        List<String> fields = selectList();         // select f, f, f from t, t, t をリストに格納
        lex.eatKeyword("from");
        Collection<String> tables = tableList();
        Predicate pred = new Predicate();
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }
        return new QueryData(fields, tables, pred);
    }
    // queryってもしかしてselectのやつだけを言う？

    private List<String> selectList(){
        List<String> L = new ArrayList<String>();
        L.add(field());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(selectList());
        }
        return L;
    }
    // 再帰的にLにフィールド名を格納？

    private Collection<String> tableList(){
        Collection<String> L = new ArrayList<String>();
        L.add(lex.eatId());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(tableList());
        }
        return L;
    }

    // 更新のコマンドに対する解析メソッド群

    public Object updateCmd() {
        if (lex.matchKeyword("insert"))
            return insert();
        else if (lex.matchKeyword("delete"))
            return delete();
        else if (lex.matchKeyword("update"))
            return modify();
        else
            return create();
    }

    private Object create() {
        lex.eatKeyword("create");
        if (lex.matchKeyword("table"))
            return createTable();
        else if (lex.matchKeyword("view"))
            return createView();
        else
            return createIndex();
    }

    // deleteコマンドの解析メソッド
    public DeleteData delete() {
        lex.eatKeyword("delete");
        lex.eatKeyword("from");
        String tblname = lex.eatId();
        Predicate pred = new Predicate();
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }
        return new DeleteData(tblname, pred);
    }

    // insertコマンドの解析メソッド群

    public InsertData insert() {
        lex.eatKeyword("insert");
        lex.eatKeyword("into");
        String tblname = lex.eatId();
        lex.eatDelim('(');
        List<String> flds = fieldList();
        lex.eatDelim(')');
        lex.eatKeyword("values");
        lex.eatDelim('(');
        List<Constant> vals = constList();
        lex.eatDelim(')');
        return new InsertData(tblname, flds, vals);
    }
    // insert into STUDENT (id, name, majorid) values (1, Sato, 13)

    private List<String> fieldList() {
        List<String> L = new ArrayList<String>();
        L.add(field());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(fieldList());
        }
        return L;
    }

    private List<Constant> constList() {
        List<Constant> L = new ArrayList<Constant>();
        L.add(constant());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(constList());
        }
        return L;
    }

    // modifyコマンド(update)の解析メソッド
    public ModifyData modify() {
        lex.eatKeyword("update");
        String tblname = lex.eatId();
        lex.eatKeyword("set");
        String fldname = field();
        lex.eatDelim('=');
        Expression newval = expression();
        Predicate pred = new Predicate();
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }
        return new ModifyData(tblname, fldname, newval, pred);
    }

    // create tableコマンドについての解析メソッド
    public CreateTableData createTable() {
        lex.eatKeyword("table");
        String tblname = lex.eatId();
        lex.eatDelim('(');
        Schema sch = fieldDefs();
        lex.eatDelim(')');
        return new CreateTableData(tblname, sch);
    }

    private Schema fieldDefs() {
        Schema schema = fieldDef();
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            Schema schema2 = fieldDefs();           // カンマ区切りのフィールド情報をスキーマに格納
            schema.addAll(schema2);
        }
        return schema;
    }

    private Schema fieldDef() {
        String fldname = field();
        return fieldType(fldname);
    }

    private Schema fieldType(String fldname) {
        Schema schema = new Schema();
        if (lex.matchKeyword("int")) {
            lex.eatKeyword("int");
            schema.addIntField(fldname);
        }
        else {
            lex.eatKeyword("varchar");
            lex.eatDelim('(');
            int strLen = lex.eatIntConstant();
            lex.eatDelim(')');
            schema.addStringField(fldname, strLen);
        }
        return schema;
    }
    // int か varchar(n)

    // create viewコマンドを解析するメソッド
    public CreateViewData createView() {
        lex.eatKeyword("view");
        String viewname = lex.eatId();
        lex.eatKeyword("as");
        QueryData qd = query();
        return new CreateViewData(viewname, qd);
    }

    // create indexコマンドを解析するメソッド
    public CreateIndexData createIndex() {
        lex.eatKeyword("index");
        String idxname = lex.eatId();
        lex.eatKeyword("on");
        String tblname = lex.eatId();
        lex.eatDelim('(');
        String fldname = field();
        lex.eatDelim(')');
        return new CrateIndexData(idxname, tblname, fldname);
    }

}
