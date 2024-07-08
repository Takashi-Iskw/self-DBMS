package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

import java.util.HashMap;
import java.util.Map;

public class StatMgr {
    private TableMgr tblMgr;
    private Map<String, StatInfo> tablestats;
    private int numcalls;

    public StatMgr(TableMgr tblmgr, Transaction tx) {
        this.tblMgr = tblmgr;
        refreshStatistics(tx);
    }

    public synchronized StatInfo getStatInfo(String tblname, Layout layout, Transaction tx) {
        numcalls++;
        if (numcalls > 100)                                     // 統計メタ情報が100回更新されたら
            refreshStatistics(tx);                              // 統計メタ情報を作り直す
        StatInfo si = tablestats.get(tblname);
        if(si == null) {
            si = calcTableStats(tblname, layout, tx);
            tablestats.put(tblname, si);
        }
        return si;
    }

    // テーブルごとの統計メタ情報を削除し、カタログから統計メタ情報のmapを作成
    private synchronized void refreshStatistics(Transaction tx) {
        tablestats = new HashMap<String, StatInfo>();
        numcalls = 0;
        Layout tcatlayout = tblMgr.getLayout("tblcat", tx);
        TableScan tcat = new TableScan(tx, "tblcat", tcatlayout);
        while(tcat.next()) {
            String tblname = tcat.getString("tblname");             // テーブルごとにテーブル名と
            Layout layout = tblMgr.getLayout(tblname, tx);                  // レイアウトを取得
            StatInfo si = calcTableStats(tblname, layout, tx);
            tablestats.put(tblname, si);
        }
        tcat.close();
    }

    private synchronized StatInfo calcTableStats(String tblname, Layout layout, Transaction tx) {
        int numRecs = 0;
        int numblocks = 0;
        TableScan ts = new TableScan(tx, tblname, layout);
        while(ts.next()) {
            numRecs++;                                          // テーブルをレコードごとにスキャンし数をカウント
            numblocks = ts.getRid().blockNumber() + 1;          // レコードIDからBlock個数を取得
        }
        ts.close();
        return new StatInfo(numblocks, numRecs);
    }
}
