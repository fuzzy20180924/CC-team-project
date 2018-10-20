# -*- coding: utf-8 -*-
"""
Created on Sat Oct 20 01:54:42 2018

Q1 QRcode encoding decoding stress test

@author: MomWithMe
"""
import numpy as np
import QREncoder
import QRDecoder
from SharedConstants import DECODING_MATRIX_32by32

DECODE_SIZE = 32


# --- CORE TEST --- #
def stressTest(n=100, debug=0):
    for i in range(n):
        msg = generate_string()

        # this output needs to be the 0 - 1 matrix BEFORE
        # logistic map XOR
        encoded_string, origmatrix = QREncoder.QRencoder().encode(msg, pre_logit=True)

        # enlarge the size and generate random noise
        enlarged_string = enlarge_and_random(origmatrix)

        # decode
        decodedmsg = QRDecoder.QRDecode(enlarged_string)

        # output
        if debug == 1:
            print(msg)
            if debug == 2:
                print(encoded_string)
                print(enlarged_string)
            print(decodedmsg)
        print(decodedmsg == msg)


def generate_string():
    """
    generate random string for QRcode encoding and decoding
    """
    length = np.random.randint(1, 23)
    outstring = ''
    for i in range(length):
        outstring += chr(np.random.randint(0, 256))
    return outstring


def enlarge_and_random(m_in):
    size = m_in.shape[0]
    # generate orientation
    rotation = np.random.randint(0, 4)
    for i in range(rotation):
        m_in = np.rot90(m_in)
    # generate position of QR matrix in the larger matrix
    x = np.random.randint(0, DECODE_SIZE-size)
    y = np.random.randint(0, DECODE_SIZE-size)
    # embed the QRcode in larger matrix
    m_enlarge = np.random.randint(2, size=(DECODE_SIZE, DECODE_SIZE))
    m_enlarge[x:(x+size), y:(y+size)] = m_in
    # XOR with logistic mapping matrix
    m_enlarge = m_enlarge ^ DECODING_MATRIX_32by32
    # change matrix to hex string
    m1d = m_enlarge.ravel()
    outstring = ''
    for i in range(m1d.shape[0]//8):
        hexstring = ''.join([str(x) for x in m1d[i*8:(i*8+8)]])
        hexstring = '0x' + format(int(hexstring, 2), '08x')
        outstring += hexstring
    return outstring

if __name__ == "__main__":
    try:
        print("Please input number of runs (default: 100):", end='')
        n = int(input())
        if not n:
            raise KeyError
    except:
        print('input value not valid, using default = 100 runs')
        n = 100
    try:
        print("Please choose debug level (0, 1, 2): ", end='')
        debug = int(input())
        if debug not in [0, 1, 2]:
            raise ValueError
    except:
        print('invalid debug level, using default (0)')
        debug = 0

    stressTest(n=n, debug=debug)
