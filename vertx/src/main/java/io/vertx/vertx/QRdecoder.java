package io.vertx.vertx;

public class QRdecoder {
    /**
     * The constructor.
     */
    public QRdecoder() {

    }

    /**
     * Some hard coded constants not in shared classes
     */
    private static final String testString = "0xad7341aa0xa79b5d880xca0c3d010x3f0d3f5b0x10f8fc3f0x7c2441fb0x66df9e120x5247a48d0x4e2870510xd16dbf930x44d97670x2b1389550x205ce8720x62213c320x101f6b6a0x8ddef7d30xd46e6f750x1d5eff70xa95d20110xcb8b4b150xe16aa8bf0x973d69fb0x371969350x3712e4ce0x8b9d3a20x58629c390x76c42b020xc141b9900x2392dbe30x93d6c140x781856b50xff64420c";
    private static final String testDecodedString = "Cj3DeU";
    private static final int posDetArray[] = {127, 65, 93, 93, 93, 65, 127};
    private static final int ArraySize = 32;
    private static final int SmallCorner = 12;
    private static final int BigCorner = 8;
    private static final String ErrMsg = "No QR message found";

    /**
     * Class-wide constants imported from shared classes
     */
    private static final Integer logiMapArray[] = LogiMap.logiMapArray;

    /**
     * TODO: comment out or delete main() after done
     * main() function used for local debugging
     *
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        Boolean istest = true;
        String sampleEncodedString = "";
        if (istest) {
            Long s = System.currentTimeMillis();
            for (int i = 0; i < 1; i++) {
                sampleEncodedString = decode(testString);
            }
            System.out.println(System.currentTimeMillis() - s);
            System.out.println(sampleEncodedString);
            System.out.println(testDecodedString);
            System.out.println(testDecodedString.equals(sampleEncodedString));
        } else {
            System.out.println("Set istest to true to run the test");
        }
    }

    /**
     * decode(final String instr) - main function for decoding
     * public interface for cross class references
     *
     * @param inStr
     * @return
     * @throws Exception
     */
    public static String decode(final String inStr) throws Exception {
        String outStr;
        // step 0: split the string into hex string
        String inHexStr[] = inStr.split("0x");  // first input will be empty, remember to skip it
        // step 1: reverse the logistic map
        Integer inHexVal[] = logiMapInv(inHexStr);
        // step 2: search for the QR pattern
        String QRString = searchPattern(inHexVal);
        if (QRString.length() == 0) {
            // QR pattern not found, return an empty string
            return ErrMsg;
        }
        // step 3: extract the encoded string
        Integer rotation = Integer.valueOf(QRString.substring(0, 1));  //first byte indicates the rotation status
        Integer size = Integer.valueOf(QRString.substring(1, 2));
        QRString = QRString.substring(2);  // now the string is purely QR 0-1 bit string
        // step 4: extract the encoded message
        outStr = extractMessage(QRString, rotation, size);
        // return output
        return outStr;
    }

    /**
     * logiMapInv(String []) - Invert the logistic map of incoming array of hex string
     * output is an array of 32-bit integers
     *
     * @param inHexStr
     * @return
     * @throws Exception
     */
    private static Integer[] logiMapInv(String[] inHexStr) throws Exception {
        int n = inHexStr.length;
        Integer outHexVal[] = new Integer[ArraySize];
        for (int i = 1; i < n; i++) {
            int k = 4*(i-1);
            Integer logiDecode = (logiMapArray[k] << 24)
                    + (logiMapArray[k + 1] << 16)
                    + (logiMapArray[k + 2] <<8)
                    + logiMapArray[k + 3];
            outHexVal[i - 1] = Integer.parseUnsignedInt(inHexStr[i], 16) ^ logiDecode;
        }
        return outHexVal;
    }

    /**
     * searchPattern(Integer []) - search for (possibly rotated QR pattern within the 32-bit integer
     * array of 32 elements), should return a bit string of the flattened QR pattern (in rotated status)
     *
     * @param inHexVal
     * @return
     * @throws Exception
     */
    private static String searchPattern(Integer[] inHexVal) throws Exception {
        int row = 0;
        int col = 0;
        Integer found = 0;
        int version = 0;
        // search for 21 by 21 pattern
        for (row = 0; row < SmallCorner; row++) {
            for (col = 0; col < SmallCorner; col++) {
                found = findCorners(inHexVal, row, col, version);
                if (found != 0) {
                    break;
                }
            }
            if (found != 0) {
                break;
            }
        }
        // search for 25 by 25 pattern, if previous search failed
        if (found == 0) {
            version = 1;
            for (row = 0; row < BigCorner; row++) {
                for (col = 0; col < BigCorner; col++) {
                    found = findCorners(inHexVal, row, col, version);
                    if (found != 0) {
                        break;
                    }
                }
                if (found != 0) {
                    break;
                }
            }
        }
        // extract the QR array from the larger array
        if (found == 0) {
            return "";
        }
        return String.valueOf(found) + String.valueOf(version) + extractQRArray(inHexVal, row, col, version);
    }

    /**
     * extractMessage(String, int rotation, int size) - extract hidden message within 0-1 QR string
     *
     * @param QRString: 0-1 bit string from a flattened QR matrix
     * @param rotation: rotation indicator, 1 (not-rotated) to 4 (270 counterclockwise rotated)
     * @param size: size indicator of the QR string (0 for 21 by 21; 1 for 25 by 25)
     * @return The final, hidden message
     */
    private static String extractMessage(String QRString, int rotation, int size) {
        StringBuilder finalStrBuilder = new StringBuilder();
        StringBuilder currCharBuilder = new StringBuilder();
        StringBuilder sizeBuilder = new StringBuilder();
        int startIdx;
        Integer fillPos[] = (size == 0) ? SharedConstants21.fillPosList[rotation-1]: SharedConstants25.fillPosList[rotation-1];
        // extract size of the message first
        for (int i = 0; i < 8; i++) {
            startIdx = fillPos[i];
            sizeBuilder.append(QRString.substring(startIdx, startIdx+1));
        }
        int messageSize = Integer.parseInt(sizeBuilder.toString(), 2);
        // extract message
        int s = 8;  //starting position of first character bit string in QRstring
        for (int j = 0; j < messageSize; j++) {
            s = 8 + j * 16;
            for (int i = 0; i < 8; i++) {
                startIdx = fillPos[s + i];
                currCharBuilder.append(QRString.substring(startIdx, startIdx+1));

            }
            int charVal = Integer.valueOf(currCharBuilder.toString(), 2);
            finalStrBuilder.append((char) charVal);
            currCharBuilder.setLength(0);  // clear the builder for next character
        }
        return finalStrBuilder.toString();
    }
    /**
     * findCornersSmall() - search for 21 by 21 QR pattern
     * returns integer value between 0 and 4
     * 0: not found
     * 1 - 4: found and returned value stands for rotation (1: original, 4: 270 degrees counter-clockwise rotated)
     *
     * @param inHexVal
     * @param row
     * @param col
     * @return
     */
    private static int findCorners(Integer[] inHexVal, int row, int col, int version) {
        int lineCnt = 0;
        int i = row;
        int matchCnt = checkLine(inHexVal, i, row, col, version);
        while (matchCnt > 0) {
            lineCnt += 1;
            if (lineCnt >= 7) { break; }
            i++;
            //TODO: should I check the consistency of matchCnt across rows?
            //TODO: i.e., if previous row returns 1 or 2 or 3, then the next row should return the
            //TODO: the same value as well.
            matchCnt = checkLine(inHexVal, i, row, col, version);
        }
        if (lineCnt == 7) {
            // some pattern found in the upper half region
            if (matchCnt < 3) {
                // only one position detection pattern in the upper half
                // need to check the lower half for consistency
                Integer lowermatchCnt = findCornersLower(inHexVal, row + 14 + version * 4, col, version);
                if (lowermatchCnt != 3) {
                    // if lower half doesn't contain two patterns, not found
                    return 0;
                }
                return matchCnt+1;
            } else {
                // two position detection pattern found in the upper half
                // need to check lines in the bottom half to determine
                // rotation
                matchCnt = findCornersLower(inHexVal, row + 14, col, version);
                if (matchCnt == 1) {
                    return 1;  // unrotated
                } else if (matchCnt == 2) {
                    return 4; // 270 deg cc rotated
                } else {
                    return 0;
                }
            }
        } else {
            // no position detection pattern found in the upper half,
            // therefore QR pattern must not start from current row and column
            return 0;
        }
    }

    private static Integer findCornersLower(Integer[] inHexVal, int row, int col, int version) {
        int lineCnt = 0;
        int i = row;
        int matchCnt = checkLine(inHexVal, i, row, col, version);
        while (matchCnt > 0) {
            lineCnt += 1;
            if (lineCnt >= 7) { break; }
            i++;
            //TODO: should I check the consistency of matchCnt across rows?
            //TODO: i.e., if previous row returns 1 or 2 or 3, then the next row should return the
            //TODO: the same value as well.
            matchCnt = checkLine(inHexVal, i, row, col, version);
        }
        if (lineCnt == 7) {
            return matchCnt;
        } else {
            return 0;
        }
    }

    /**
     * checkLineSmall() - check if current line/row/32-bit integer contains the required position
     * detection pattern, and how many; returns 0, 1 (left pattern) or 2 (right pattern) or 3 (both)
     *
     * @param inHexVal
     * @param i
     * @param row
     * @param col
     * @return
     */
    private static int checkLine(Integer[] inHexVal, int i, int row, int col, int version) {
        int target = posDetArray[i - row];  // this is the numerical value corresponding to a row in a position detection pattern
        int line = inHexVal[i];  // this is the ith 32-bit integer, or ith row in the QR matrix
        int targetLeft = target << (25 - col);  // this marks the position of the left position detection pattern
        int targetRight = (version == 0) ? (target << (11 - col)) : (target << (7 - col));  // this marks the position of the right pos-detection pattern
        int match = 0;
        if ((line & targetLeft) == targetLeft) {
            // left pattern found on this row/line
            match += 1;
        }
        if ((line & targetRight) == targetRight) {
            // right pattern found on this row/line
            match += 2;
        }
        return match;
    }

    /**
     * extractQRArray() - after finding the QR pattern in original 32 by 32 matrix,
     * extract the pattern out as one bit string
     * @param inHexVal
     * @param row
     * @param col
     * @param version
     * @return
     */
    private static String extractQRArray(Integer[] inHexVal, int row, int col, int version){
        int Length = (version == 0) ? 21 : 25;
        int endRow = row + Length;
        int startIdx;
        StringBuilder outStrBuilder = new StringBuilder();
        for (int i = row; i < endRow; i++) {
            String currHexStr = Integer.toBinaryString(inHexVal[i]);
            while (currHexStr.length() < ArraySize) {
                currHexStr = "0" + currHexStr;
            }
            outStrBuilder.append(currHexStr.substring(col, col+Length));
        }
        return outStrBuilder.toString();
    }

}
