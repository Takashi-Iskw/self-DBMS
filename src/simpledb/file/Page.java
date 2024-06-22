package simpledb.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {
    private ByteBuffer bb;
    public static final Charset CHARSET = StandardCharsets.US_ASCII;

    public Page(int blocksize) {
        bb = ByteBuffer.allocateDirect(blocksize);
    }

    public Page(byte[] b){
        bb = ByteBuffer.wrap(b);
    }

    public int getInt(int offset) {
        return bb.getInt(offset);
    }

    public void setInt(int offset, int n) {
        bb.putInt(offset, n);
    }


    public byte[] getBytes(int offset) {
        bb.position(offset);
        int length = bb.getInt();
        byte[] b = new byte[length];
        bb.get(b); // 長さ分の文字列をbyte配列として取得？
        return b;
    }
    // ByteBufferの働きよく分からんが、String(byte[], CHARSET)でbyte配列から文字列に変換可能

    public void setBytes(int offset, byte[] b) {
        bb.position(offset);
        bb.putInt(b.length);
        bb.put(b);
    }
    // 何に使う？setStringとの使い分けは？

    public String getString(int offset) {
        byte[] b = getBytes(offset);
        return new String(b, CHARSET);
    }

    public void setString(int offset, String s) {
        byte[] b = s.getBytes(CHARSET);
        setBytes(offset, b);
    }
    // 文字列はそのままでなくbyte配列として記述する

    public static int maxLength(int strlen) {
        float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES + (strlen * (int)bytesPerChar);
    }

    ByteBuffer contents() {
        bb.position(0);
        return bb;
    }

}
