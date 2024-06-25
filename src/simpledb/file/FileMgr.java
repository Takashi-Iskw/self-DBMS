package simpledb.file;

import java.io.*;
import java.nio.*;
import java.util.*;

public class FileMgr {
    private File dbDirectory;
    private int blocksize;
    private boolean isNew;
    private Map<String, RandomAccessFile> openFiles = new HashMap<>();

    public FileMgr(File dbDirectory, int blocksize) {
        this.dbDirectory = dbDirectory;
        this.blocksize = blocksize;
        isNew = !dbDirectory.exists();

        if (isNew)
            dbDirectory.mkdirs();

        //materialized operators(13章)によって作成された可能性があるファイルを削除
        for (String filename : dbDirectory.list())
            if (filename.startsWith("temp"))
                new File(dbDirectory, filename).delete();
    }

    // 実際のブロックの内容を取得
    public synchronized void read(BlockId blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blocksize);
            f.getChannel().read(p.contents());
        }
        catch (IOException e) {
            throw new RuntimeException("cannot read block " + blk);
        }
    }
    // p.contents()とは？ p.contents (バッファのこと) にfの情報が記述されるっぽい。

    public synchronized void write (BlockId blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blocksize);
            f.getChannel().write(p.contents());
        }
        catch (IOException e) {
            throw new RuntimeException("cannot write block " + blk);
        }
    }
    // writeメソッドはp.contents (バッファ) の内容がfに書き込まれる。readの逆らしい

    // fileの最後に空のbyte配列を追加？
    public synchronized BlockId append(String filename) {
        int newblknum = length(filename);
        BlockId blk = new BlockId(filename, newblknum);
        byte[] b = new byte[blocksize];
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blocksize);
            f.write(b);
        } catch (IOException e) {
            throw new RuntimeException("block" + blk + "をappendできませんでした");
        }
        return blk;
    }
    // synchronizedについて：read, write, appendは同期されており、一度に実行できるのは1つだけ

    // ファイルのブロックの数を取得
    public int length(String filename) {
        try {
            RandomAccessFile f = getFile(filename);
            return (int)(f.length() / blocksize);
        } catch (IOException e) {
            throw new RuntimeException(filename + "にアクセスできませんでした");
        }
    }

    public boolean isNew() {
        return isNew;
    }

    public int blockSize() {
        return blocksize;
    }


    private RandomAccessFile getFile(String filename) throws IOException {
        RandomAccessFile f = openFiles.get(filename);
        if(f == null) {
            File dbTable = new File(dbDirectory, filename);
            f = new RandomAccessFile(dbTable, "rws");
            openFiles.put(filename, f);
        }
        return f;
    }
    // openFilesとは？ → HashMap
    // dbDirectoryとは？
    // File(dbDirectory, filename)は何を取得？

}
