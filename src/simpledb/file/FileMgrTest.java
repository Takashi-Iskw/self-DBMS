package simpledb.file;

import java.io.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class FileMgrTest {
    public static void main(String[] args) throws IOException {
        BlockId blk1 = new BlockId("testfile", 0);
        Page p = new Page(400);
        File testdir = new File("testdir");
        FileMgr fm = new FileMgr(testdir, 400);

        p.setInt(0, 100);
        p.setString(4, "abcde");

        fm.write(blk1, p);

        System.out.println(p.getInt(0));


    }
}
