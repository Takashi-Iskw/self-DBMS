package simpledb.file;

import java.io.*;
import java.util.*;

public class blkTest {
    public static void main(String[] args) throws IOException {
        BlockId blk = new BlockId("blktest", 0);
        BlockId blk2 = new BlockId("blktest", 1);
        BlockId blk3 = new BlockId("blktest", 0);

        System.out.println(blk.equals(blk2));
        System.out.println(blk.equals(blk3));

        System.out.println(blk.toString());
        System.out.println(blk.hashCode());

    }
}
