package simpledb.utils;

public class BitUtil {
  public static int calZeroInByte (byte b) {
    int cnt = 0;
    for (int i = 0; i < 8; i++) {
      if (((b >>> (byte) i) & (byte)1) == 0) cnt++;
    }

    return cnt;
  }
}
