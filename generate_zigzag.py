import os
import numpy as np
from matplotlib.colors import ListedColormap
import seaborn as sn
import matplotlib.pyplot as plt

pattern_folder = "patterns"
if not os.path.exists(pattern_folder):
	os.mkdir(pattern_folder)

# generate fill-in pattern for 21x21
p = -1*np.ones((21,21))

# fill in format information part
for i in range(8):
    for j in range(9):
        p[i,j] = -1
for i in range(13, 21):
    for j in range(9):
        p[i,j] = -1
for i in range(8):
    for j in range(13,21):
        p[i,j] = -1
for j in range(21):
    p[6, j] = -1
for i in range(21):
    p[i, 6] = -1
for i in range(0, 13):
    p[20-i, 20] = 2*i
    p[20-i, 19] = 2*i+1
    p[8+i, 18] = 26+2*i
    p[8+i, 17] = 26+2*i+1
    p[20-i, 16] = 52+2*i
    p[20-i, 15] = 52+2*i+1
    p[8+i, 14] = 78+2*i
    p[8+i, 13] = 78+2*i+1
for i in range(0, 14):
    p[20-i, 12] = 104+2*i
    p[20-i, 11] = 104+2*i+1
for i in range(15, 21):
    p[20-i, 12] = 104+2*i-2
    p[20-i, 11] = 104+2*i+1-2
for i in range(0, 6):
    p[i, 10] = 144+2*i
    p[i, 9] = 144+2*i+1
for i in range(7, 21):
    p[i, 10] = 142+2*i
    p[i, 9] = 142+2*i+1
for i in range(0, 5):
    p[12-i, 8] = 184+2*i
    p[12-i, 7] = 184+2*i+1
for i in range(0, 5):
    p[8+i, 5] = 194+2*i
    p[8+i, 4] = 194+2*i+1
for i in range(0, 5):
    p[12-i, 3] = 204+2*i
    p[12-i, 2] = 204+2*i+1
    
for i in range(0, 5):
    p[8+i, 1] = 214+2*i
    p[8+i, 0] = 214+2*i+1

# draw 21x21 pattern
fig,ax = plt.subplots(1,1,figsize=(10, 10))
with sn.axes_style('white'):
    sn.heatmap(p,
                cbar=False,
                annot=True,
                fmt='g',
                
                linewidths=0.1)
fig.savefig(os.path.join(pattern_folder, "21x21.png"), dpi=600)

# generate output
zeroone_pattern = np.ones((21,21))
zeroone_pattern[p>=0] = 0

nz = (1-zeroone_pattern).nonzero()
row_ind = nz[0]
col_ind = nz[1]
position = np.array([(row_ind[i], col_ind[i]) for i in range(len(row_ind))])
order = np.argsort([p[pos[0],pos[1]] for pos in position])
fill_in_position_array = position[order]
np.save(os.path.join(pattern_folder, "21x21_zeroone_pattern.npy"), zeroone_pattern)
np.save(os.path.join(pattern_folder,"21x21_fill_in_position_array.npy"), fill_in_position_array)


#generate fill-in pattern for 25x25
q = -1*np.ones((25,25))
for i in range(8):
    for j in range(9):
        q[i,j] = -1
for i in range(17, 25):
    for j in range(9):
        q[i,j] = -1
for i in range(8):
    for j in range(17,25):
        q[i,j] = -1
for j in range(25):
    q[6, j] = -1
for i in range(25):
    q[i, 6] = -1
for i in range(16, 21):
    for j in range(15, 21):
        q[i,j] = -1
for i in range(0, 17):
    q[24-i, 24] = 2*i
    q[24-i, 23] = 2*i+1
    q[8+i, 22] = 34+2*i
    q[8+i, 21] = 34+2*i+1
for i in range(0, 4):
    q[24-i, 20] = 68+2*i
    q[24-i, 19] = 68+2*i+1
for i in range(9, 17):
    q[24-i, 20] = 76+2*i-18
    q[24-i, 19] = 76+2*i+1-18
for i in range(0, 8):
    q[8+i, 18] = 92+2*i
    q[8+i, 17] = 92+2*i+1
for i in range(0, 4):
    q[21+i, 18] = 108+2*i
    q[21+i, 17] = 108+2*i+1
for i in range(0, 4):
    q[24-i, 16] = 116+2*i
    q[24-i, 15] = 116+2*i+1

for i in range(9, 18):
    q[24-i, 16] = 124+2*i-18
    q[24-i, 15] = 124+2*i+1-18
for i in range(19, 25):
    q[24-i, 16] = 124+2*i-18-2
    q[24-i, 15] = 124+2*i+1-18-2

for i in range(0, 6):
    q[i, 14] = 154+2*i
    q[i, 13] = 154+2*i+1
for i in range(7, 25):
    q[i, 14] = 154+2*i-2
    q[i, 13] = 154+2*i+1-2

for i in range(0, 18):
    q[24-i, 12] = 202+2*i
    q[24-i, 11] = 202+2*i+1
for i in range(19, 25):
    q[24-i, 12] = 202+2*i-2
    q[24-i, 11] = 202+2*i+1-2

for i in range(0, 6):
    q[i, 10] = 250+2*i
    q[i, 9] = 250+2*i+1
for i in range(7, 25):
    q[i, 10] = 250+2*i-2
    q[i, 9] = 250+2*i+1-2

for i in range(0, 9):
    q[16-i, 8] = 298+2*i
    q[16-i, 7] = 298+2*i+1
for i in range(0, 9):
    q[8+i, 5] = 316+2*i
    q[8+i, 4] = 316+2*i+1
for i in range(0, 9):
    q[16-i, 3] = 334+2*i
    q[16-i, 2] = 334+2*i+1
for i in range(0, 9):
    q[8+i, 1] = 352+2*i
    q[8+i, 0] = 352+2*i+1

# draw 25x25 pattern
from matplotlib.colors import ListedColormap
fig,ax = plt.subplots(1,1,figsize=(10, 10))
with sn.axes_style('white'):
    sn.heatmap(q,
                cbar=False,
                annot=True,
                fmt='g',
                
                linewidths=0.1)
fig.savefig(os.path.join(pattern_folder, "25x25.png"), dpi=600)

# generate output
zeroone_pattern = np.ones((25,25))
zeroone_pattern[q>=0] = 0
nz = (1-zeroone_pattern).nonzero()
row_ind = nz[0]
col_ind = nz[1]
position = np.array([(row_ind[i], col_ind[i]) for i in range(len(row_ind))])
order = np.argsort([q[pos[0],pos[1]] for pos in position])
fill_in_position_array = position[order]
np.save(os.path.join(pattern_folder, "25x25_zeroone_pattern.npy"), zeroone_pattern)
np.save(os.path.join(pattern_folder, "25x25_fill_in_position_array.npy"), fill_in_position_array)