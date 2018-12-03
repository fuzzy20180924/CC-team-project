package io.vertx.vertx;

// import org.apache.arrow.flatbuf.Bool;
// import org.apache.commons.lang.ArrayUtils;

import java.math.BigInteger;
import java.util.Arrays;

public class QRencoder {
    /**
     *  The constructor.
     */
    // public QRencoder() {

    // }

    /**
     *  Sample results
     *  TODO: delete these two lines during actual test
     */
    private static final String testString21 = "CC Team";
    private static final String testEncodedString21 = "0x66d92b800x5bc76d830x121a7fa60x51c111870x3a5f3ca30x8be36a130xedb223a0xfc8e98780x33bf50de0x2e8709700x545a2d0f0xecef7ae0x461175cd0xff132a";
    private static final String testString25 = "CC Team is awesome!";
    private static final String testEncodedString25 = "0x66ede8530xb3b981a10xed18e4040xa4a0026c0xd039db570x21976f0d0xed168440xfdce22bf0xd67e47ec0x2171a0600x2a1a95010x875f3f480x78347f130x886ccc430xc90f439a0x331f54900x7bbcbf030x20d731250xc555223e0x15858";
    /**
     *  Hardcoded constants
     */
    private static final Character tailSequence[] = {'1','1','1','0','1','1','0','0','0','0','0','1','0','0','0','1'};
    private static final Integer tailSeqLength = 16;
    private static final Integer logiArray[] = LogiMap.logiMapArray;
    private static final Integer logiTail21 = 0;  // this is the MSB of the 55th value in logistic map
    private static final Integer logiTail25 = 0;

    /**
     * TODO: comment out or delete this function after done
     * main() function used for local debugging
     * @param args
     * @throws Exception
     */
    // public static void main(final String[] args) throws Exception {
    //     Boolean istest = true;
    //     String sampleEncodedString = "";
    //     if (istest) {
    //         Long s = System.currentTimeMillis();
    //         for (int i = 0; i < 10000; i++) {
    //             sampleEncodedString = encode(testString21);
    //         }
    //         System.out.println(System.currentTimeMillis() - s);
    //         for (int i = 0; i < 10000; i++) {
    //             sampleEncodedString = encode(testString25);
    //         }
    //         System.out.println(System.currentTimeMillis() - s);
    //         System.out.println(sampleEncodedString);
    //         System.out.println(testEncodedString25);
    //         System.out.println(testEncodedString25.equals(sampleEncodedString));
    //     } else {
    //         System.out.println("Set istest to true to run the test");
    //     }
    // }

    /**
     * encode(final String inString) - core encoding function
     * @param inString
     * @return
     * @throws Exception
     */
    public static String encode(final String inString) throws Exception {
        int n = inString.length();
        if (n <= 13) {
            // 21 by 21 QR code
            return encodeHelper(inString, n, 1);
        } else if (n <= 22) {
            // 25 by 25 QR code
            return encodeHelper(inString, n, 2);
        } else {
            // input string too long
            return "";
        }

    }

    /**
     * encodeHelper(String) - helper function for encoding
     * @param inString
     * @return
     * @throws Exception
     */
    private static String encodeHelper(final String inString, int length, int version) throws Exception {
        // get the initialized QR array
        char QRArray[];
        Integer fillPosArray[];
        if (version == 1){
            QRArray = SharedConstants21.initialQR21;
            fillPosArray = SharedConstants21.fillPos21;
        } else {
            QRArray = SharedConstants25.initialQR25;
            fillPosArray = SharedConstants25.fillPos25;
        }
        // get size byte
        String bitString = Integer.toString(length, 2);
        // fill the size byte up to 8-bit
        while (bitString.length() < 8) {
            bitString = '0' + bitString;
        }
        // form bit string from incoming message
        int m = inString.length();
        for (int i = 0; i < m; i++) {
            bitString += AscTable.ascList[(int)inString.charAt(i)];
        }
        // fill in QR array
        int lenString = bitString.length();
        for (int i = 0; i < lenString; ++i) {
            QRArray[fillPosArray[i]] = bitString.charAt(i);
        }
        int n = fillPosArray.length;
        // fill the rest, if any, with fixed pattern
        for (int i = lenString; i < n; i++) {
            QRArray[fillPosArray[i]] = tailSequence[(i - lenString) % tailSeqLength];
        }
        // logistic map encoding, each element is 1-byte integer [0, 255]
        // encode the bit String into 32-bit hex value string
        String encString = new String();
        int p = QRArray.length/32;
        int i;
        for (i = 0; i < p; i++) {
            encString += logiConvHex(QRArray, i);
        }
        // deal with the tail of the QR encoded string
        if (version == 1) {
            encString += logiConvHexTail21(QRArray, i, 441);
        } else {
            encString += logiConvHexTail25(QRArray, i, 625);
        }
        return encString;
    }

    /**
     *  Helper function for logistic map and hex string conversion
     */
    private static String logiConvHex(char Arr[], int i) {
        int j = i*32; // index into QRArray
        int k = i*4;  // index into logiArray
        Integer logicode = (logiArray[k] << 24)
                + (logiArray[k+1] << 16)
                + (logiArray[k+2] << 8)
                + logiArray[k+3];
        Long currByte = new BigInteger(new String(Arrays.copyOfRange(Arr, j, j+32)), 2).longValue();
        currByte = currByte ^ logicode;
        // convert numeric value to String then zero-pad till 8 hex digits
        String outString = Long.toHexString(currByte); // this could be 64 bit long
        // zero-pad up to 8 digits, in case the bit value is small
        int len = outString.length();
        if (len > 8) {
            return "0x" + outString.substring(len-8, len);
        } else {
            return "0x" + outString;
        }
    }

    /**
     * Helper function of encoded the "tail" part (less than 32 digits) of the bit string
     * For 21 by 21 QRcode
     * @param Arr
     * @param i
     * @param n
     * @return
     */
    private static String logiConvHexTail21(char Arr[], int i, int n) {
        int j = i*32; // index into QRArray
        int k = i*4;  // index into logiArray
        Integer last1 = Integer.parseInt(new String(Arrays.copyOfRange(Arr, j, j+8)), 2);
        Integer last2 = Integer.parseInt(new String(Arrays.copyOfRange(Arr, j+8, j+16)), 2);
        Integer last3 = Integer.parseInt(new String(Arrays.copyOfRange(Arr, j+16, j+24)), 2);
        Integer last4 = Integer.parseInt(new String(Arrays.copyOfRange(Arr, j+24, n)), 2); // this should just 1 bit
        Integer currByte = ((logiArray[k] ^ last1) << 17)
                + ((logiArray[k+1] ^ last2) << 9)
                + ((logiArray[k+2] ^ last3) << 1)
                + (logiTail21 ^ last4);
        // convert numeric value to String then zero-pad till 8 hex digits
        String outString = Integer.toHexString(currByte); // this will be 64 bit long
        return "0x" + outString;
    }

    /**
     * Helper function of encoded the "tail" part (less than 32 digits) of the bit string
     logiArray[k]     * For 25 by 25 QRcode
     * @param Arr
     * @param i
     * @param n
     * @return
     */
    private static String logiConvHexTail25(char Arr[], int i, int n) {
        int j = i*32; // index into QRArray
        int k = i*4;  // index into logiArray
        Integer last1 = Integer.parseInt(new String(Arrays.copyOfRange(Arr, j, j+8)), 2);
        Integer last2 = Integer.parseInt(new String(Arrays.copyOfRange(Arr, j+8, j+16)), 2);
        Integer last3 = Integer.parseInt(new String(Arrays.copyOfRange(Arr, j+16, n)), 2); // should be just 1 bit
        Integer currByte = ((logiArray[k] ^ last1) << 9)
                + ((logiArray[k+1] ^ last2) << 1)
                + (logiTail25 ^ last3);
        // convert numeric value to String then zero-pad till 8 hex digits
        String outString = Integer.toHexString(currByte); // this will be 64 bit long
        return "0x" + outString;
    }
}
