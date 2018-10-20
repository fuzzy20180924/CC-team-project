# -*- coding: utf-8 -*-
import numpy as np
import os
from itertools import cycle
import SharedConstants
#import generate_zigzag
from functools import reduce

PATTERN_FOLDER = "patterns"
POS_21x21 = "21x21_fill_in_position_array.npy"
POS_25x25 = "25x25_fill_in_position_array.npy"

class EncoderError(Exception):
    def __init__(self, *msg):
            self.args = msg
            
class QRencode(object):
    def __init__(self):
        self.POS_21x21, self.POS_25x25 = self._generate_patterns()
        LOGIMAP = SharedConstants.LOGIMAP.copy()
        self.logit_bits = np.zeros((79,8), dtype=np.uint8)
        for ind in range(79):
            self.logit_bits[ind] = np.array([int(i) for i in format(LOGIMAP[ind],'08b')][::-1])
        self.BASE_21x21 = np.genfromtxt('patterns/21x21_special_pattern.csv', delimiter=',')
        self.BASE_21x21[self.BASE_21x21 >=0] = 0
        self.BASE_21x21[self.BASE_21x21 == -1] = 1
        self.BASE_21x21[self.BASE_21x21 == -2] = 0
        self.BASE_21x21 = self.BASE_21x21.astype(np.uint8)
        self.BASE_25x25 = np.genfromtxt('patterns/25x25_special_pattern.csv', delimiter=',')
        self.BASE_25x25[self.BASE_25x25 >= 0] = 0
        self.BASE_25x25[self.BASE_25x25 == -1] = 1
        self.BASE_25x25[self.BASE_25x25 == -2] = 0
        self.BASE_25x25 = self.BASE_25x25.astype(np.uint8)
        
    def _generate_patterns(self):
        PATTERN_FOLDER = "patterns"
        POS_21x21 = "21x21_fill_in_position_array.npy"
        POS_25x25 = "25x25_fill_in_position_array.npy"
        ZO_21x21 = "21x21_zeroone_pattern.npy"
        ZO_25x25 = "25x25_zeroone_pattern.npy"
        if not os.path.exists(
            os.path.join(PATTERN_FOLDER, POS_21x21)):
            generate_zigzag.generate_21x21_patterns(False)
        if not os.path.exists(
            os.path.join(PATTERN_FOLDER, POS_25x25)):
            generate_zigzag.generate_25x25_patterns(False)
        POS_21x21 = np.load(os.path.join(PATTERN_FOLDER, POS_21x21))
        POS_25x25 = np.load(os.path.join(PATTERN_FOLDER, POS_25x25))
        return POS_21x21, POS_25x25
        
    def _to_ascii(self, input_msg):
        try:
            pass #input_msg.decode("ascii")
        except UnicodeDecodeError:
            print("Invalid message={}".format(input_msg)) 
            raise EncoderError(input_msg)
        except AttributeError:
            print("Ivalid message={}".format(input_msg))
            raise EncoderError(input_msg)
        return [ord(c) for c in input_msg]
    
    def _get_error_correcting_code(self, ascii_list):
        correcting_code = []
        for c in ascii_list:
            correcting_code.append(reduce(lambda x,y: int(x)^int(y), format(c,'08b')))
        return correcting_code
    
    def encode(self, input_msg, pre_logit=False):
        ascii_list = self._to_ascii(input_msg)
        num_byte = len(input_msg)
        if num_byte <= 13:
            pattern = self.BASE_21x21.copy()
            POS = self.POS_21x21.copy()
        elif 14 <= num_byte and num_byte <= 22:
            pattern = self.BASE_25x25.copy()
            POS = self.POS_25x25.copy()
        else:
            raise EncoderError("Invalid length")
        correcting_code = self._get_error_correcting_code(ascii_list)
        ascii_correcting = 2*num_byte*[0]
        ascii_correcting[::2] = ascii_list
        ascii_correcting[1::2] = correcting_code
        bit_seq = reduce(lambda x,y:x+y, [format(i, '08b') for i in [num_byte]+ascii_correcting])
        ind = 0
        len_bit_seq = len(bit_seq)

        while ind < len_bit_seq:
            pos = POS[ind]
            pattern[pos[0]][pos[1]] = int(bit_seq[ind])
            ind+=1
        len_POS = len(POS)
        fill = cycle('1110110000010001')
        while ind < len_POS:
            pos = POS[ind]
            pattern[pos[0]][pos[1]] = int(next(fill))
            ind+=1
        pattern_flat = pattern.flatten()
        logi_encrypts = np.zeros(len(pattern_flat),dtype=np.uint8)
        len_pattern = len(pattern_flat)
        
        p1 = 0
        while  p1< len_pattern:    
            p2 = min(len_pattern, p1+8)
            logi_encrypts[p1:p2] = pattern_flat[p1:p2] ^ self.logit_bits[int(p1/8)][:(p2-p1)]
            p1 = p2
        if num_byte <= 13:
            output_string = ""
            for ind in range(14):
                output_string+=hex(int("".join([str(i) for i in logi_encrypts[32*ind:32*(ind+1)]]),2))
        else:
            output_string = ""
            for ind in range(20):
                output_string+=hex(int("".join([str(i) for i in logi_encrypts[32*ind:32*(ind+1)]]),2))
        if pre_logit:
            return output_string, pattern
        else:
            return output_string

# --- Test --- #
# comment out during deployment
if __name__ == "__main__":
    encoder = QRencoder()
    sample1 = "CC Team"
    ref_out1 = "0x66d92b800x5bc76d830x121a7fa60x51c111870x3a5f3ca30x8be36a130xedb223a0xfc8e98780x33bf50de0x2e8709700x545a2d0f0xecef7ae0x461175cd0xff132a"
    output_string = encoder.encode(sample1)
    print("test case    ={}".format(sample1))
    print("output_string={}".format(output_string))
    print("ref    string={}".format(ref_out1))
    print("Test   result={}".format(ref_out1==output_string))
    sample2 = "CC Team is awesome!"
    ref_out2 = "0x66ede8530xb3b981a10xed18e4040xa4a0026c0xd039db570x21976f0d0xed168440xfdce22bf0xd67e47ec0x2171a0600x2a1a95010x875f3f480x78347f130x886ccc430xc90f439a0x331f54900x7bbcbf030x20d731250xc555223e0x15858"
    output_string = encoder.encode(sample2)
    print("test case    ={}".format(sample2))
    print("output_string={}".format(output_string))
    print("ref    string={}".format(ref_out2))
    print("Test   result={}".format(ref_out2==output_string))
    
    
    