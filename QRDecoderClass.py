# -*- coding: utf-8 -*-
"""
Created on Fri Oct 19 15:04:04 2018

@author: MomWithMe
"""
# --- package loading --- #
import numpy as np
import re
from SharedConstants import LIST21, LIST25, DECODING_MATRIX_32by32
from SharedConstants import POS21, POS25
from SharedConstants import SELECT21, SELECT25


# --- ERROR MESSAGES --- #
ERROR_HEADER = "DECODER ERROR: "
ERROR_INPUT = "input encoding string does not contain "
ERROR_INPUT = ERROR_INPUT + "exactly 32 hex values"
ERROR_WRONGSIZE = "payload size larger than payload string"
ERROR_NOMATCH = "can't find QRcode in incoming hex string"
ERROR_MULTIMATCH = "mulitple QRcode patterns found. weird..."


# --- Core --- #
class QRdecode(object):
    def __init__(self):
        self.HEX_SEP = '0x'
        self.FORMAT_HEX = '032b'
        self.BIN_DEC_ARRAY = np.array([128, 64, 32, 16, 8, 4, 2, 1])
        self.LIST21 = LIST21
        self.LIST25 = LIST25
        self.DECODING_MATRIX = DECODING_MATRIX_32by32
        self.POS21 = POS21
        self.POS25 = POS25
        self.SELECT21 = SELECT21
        self.SELECT25 = SELECT25

    def decode(self, encoding_string):
        # STEP 0: generate 0-1 matrix from input string
        status, InputMatrix = self.__generate_matrix(encoding_string)
        # STEP 1: decode using logistic map
        if not status:
            return False, ERROR_HEADER + ERROR_INPUT
        InputMatrix = self.__decode_logi(InputMatrix)
        # STEP 2: pattern matching
        status, result = self.__pattern_matching(InputMatrix)
        if not status:
            if result:
                return False, ERROR_HEADER + ERROR_MULTIMATCH
            else:
                return False, ERROR_HEADER + ERROR_NOMATCH
        # STEP 3: extract payload
        status, Payload = self.__extract_payload(result)
        if not status:
            if Payload:
                return False, ERROR_HEADER + ERROR_WRONGSIZE
        return True, Payload

    # --- Helper Functions --- #
    def __generate_matrix(self, inputstring):
        """
        given the input string, generate a 32 by 32 1-0 matrix
        """
        # split input string at "0x" and convert into
        # a list of binary string
        binlist = [format(int(x, 16), self.FORMAT_HEX) for x
                   in inputstring.split(self.HEX_SEP)[1:]]
        # assert the string length is 32 by 32
        if (len(binlist) != 32):
            return False, None
        # convert to 2-D 1-0 matrix
        InputMatrix = np.array([[int(x) for x in y] for y in binlist])

        return True, InputMatrix

    def __decode_logi(self, InputMatrix):
        """
        generate decoding matrix from logistic map
        logistic map matrix imported from "SharedConstants.py"
        """
        return InputMatrix ^ self.DECODING_MATRIX

    def __pattern_matching(self, matrix):
        """
        given input 32 by 32 matrix, output the found QR matrix
        size could be 21 by 21, or 25 by 25
        current implementation doesn't consider the possibility
        of multiple QR pattern matches (with different sizes or
        orientations), assuming it's not possible.
        the code returns the first match found.

        function returns, in case of a match, a tuple of 3 elements
        (sizetype, 0-1 matrix for QRcode in its original position)
        """
        # convert the matrix into 1D 0-1 string
        m1d = matrix.ravel()
        mstring = ''.join((str(x) for x in m1d))
        match = []
        sizetype = -1
        # find pattern
        for i in range(4):
            match = re.findall(self.LIST21[i], mstring)
            if match:
                # found
                sizetype = 0
                break
            match = re.findall(self.LIST25[i], mstring)
            if match:
                sizetype = 1
                break
        if sizetype == -1:
            return False, False
        if len(match) > 1:
            return False, True
        qrstring = match[0]
        # convert pattern string into 0-1 QR matrix
        qrcode = np.array([int(x) for x in qrstring])
        qrcode = qrcode[self.SELECT25] if sizetype else qrcode[self.SELECT21]
        if sizetype:
            # 25 by 25
            qrcode = qrcode.reshape(25, 25)
        else:
            qrcode = qrcode.reshape(21, 21)
        # rotate the QR matrix back to its original position
        if i:
            # rotation necessary
            for j in range(4-i):
                qrcode = np.rot90(qrcode)
        return True, (sizetype, qrcode)

    def __extract_payload(self, result):
        """
        given the input 3-element tuple, output the payload string
        the input is exactly the second output from the previous function
        __pattern_matching
        """
        sizetype, QRmatrix = result
        # pick the right size
        fill_position = self.POS25 if sizetype else self.POS21
        n = len(fill_position)
        # extract the payload as 0-1 array first
        QRarray = np.zeros(n, dtype=int)
        for i in range(n):
            x, y = fill_position[i]
            QRarray[i] = QRmatrix[x, y]
        # convert 0-1 array to string
        # get the size first
        size = QRarray[:8].dot(self.BIN_DEC_ARRAY)
        # assert that size is correct
        if size*16 > len(QRarray[8:]):
            return False, True
        # start conversion
        payload = ''
        for i in range(size):
            start = i*16 + 8
            char = QRarray[start:(start+8)].dot(self.BIN_DEC_ARRAY)
            payload = payload + chr(char)
        # done. Hooray!!!
        return True, payload


# --- Test --- #
# comment out during deployment
#if __name__ == "__main__":
#    sample_input = "0x2b23d6830x15a0de0d0x744784010x29e88070"
#    sample_input += "0xfe1adf5c0xb96061290x1127b67c0x31169043"
#    sample_input += "0xc63153140xf6e00650x92d3960b0xf59a7907"
#    sample_input += "0x704e73d40x977fd8090xf516e98a0x3e0c19f1"
#    sample_input += "0xac626d040x6a3e58650xca85aa3e0x6266b64"
#    sample_input += "0x842ddcb40x4e7c879c0x85dd21240x3afae3dc"
#    sample_input += "0xe07908a70x664685970xb38246f70x51190833"
#    sample_input += "0x40a111ee0xc12c8fd10x82984c520x4ddee6f6"
#    sample_output = "CC Team is awesome!"
#    status, output = QRdecode().decode(sample_input)
#    print(sample_output)
#    print(output == sample_output)
