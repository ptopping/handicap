import pandas as pd
import numpy as np
import random

library = pd.read_csv("c:\users\jen\downloads\Tracklist.csv")

def inkey(catalog,numtracks,mkey):
    '''
    catalog
    numtracks
    mkey
    '''
    return catalog[catalog['Key'] == mkey].sample(numtracks)

def setlist(catalog,length):
    '''
    catalog = Pandas dataframe
    length = Integer, number of tracks
    '''
    key_time_conv = {'C maj': {'Time' : 0, 'Same' : 'C maj', 'Left' : 'F maj', 'Right' : 'G maj', 'Vertical' : 'A min', 'Jump' : 'D maj'},
    'G maj': {'Time' : 1, 'Same' : 'G maj', 'Left' : 'C maj', 'Right' : 'D maj', 'Vertical' : 'E min', 'Jump' : 'A maj'},
    'D maj': {'Time' : 2, 'Same' : 'D maj', 'Left' : 'G maj', 'Right' : 'A maj', 'Vertical' : 'B min', 'Jump' : 'E maj'},
    'A maj': {'Time' : 3, 'Same' : 'A maj', 'Left' : 'D maj', 'Right' : 'E maj', 'Vertical' : 'F# min', 'Jump' : 'B maj'},
    'E maj': {'Time' : 4, 'Same' : 'E maj', 'Left' : 'A maj', 'Right' : 'B maj', 'Vertical' : 'C# min', 'Jump' : 'F# maj'},
    'B maj': {'Time' : 5, 'Same' : 'B maj', 'Left' : 'E maj', 'Right' : 'F# maj', 'Vertical' : 'G# min', 'Jump' : 'C# maj'},
    'F# maj': {'Time' : 6, 'Same' : 'F# maj', 'Left' : 'B maj', 'Right' : 'C# maj', 'Vertical' : 'D# min', 'Jump' : 'G# maj'},
    'C# maj': {'Time' : 7, 'Same' : 'C# maj', 'Left' : 'F# maj', 'Right' : 'G# maj', 'Vertical' : 'A# min', 'Jump' : 'D# maj'},
    'G# maj': {'Time' : 8, 'Same' : 'G# maj', 'Left' : 'C# maj', 'Right' : 'D# maj', 'Vertical' : 'F min', 'Jump' : 'A# maj'},
    'D# maj': {'Time' : 9, 'Same' : 'D# maj', 'Left' : 'G# maj', 'Right' : 'A# maj', 'Vertical' : 'C min', 'Jump' : 'F maj'},
    'A# maj': {'Time' : 10, 'Same' : 'A# maj', 'Left' : 'D# maj', 'Right' : 'F maj', 'Vertical' : 'G min', 'Jump' : 'C maj'},
    'F maj': {'Time' : 11, 'Same' : 'F maj', 'Left' : 'A# maj', 'Right' : 'C maj', 'Vertical' : 'D min', 'Jump' : 'G maj'},
    'A min': {'Time' : 12, 'Same' : 'A min', 'Left' : 'D min', 'Right' : 'E min', 'Vertical' : 'C maj', 'Jump' : 'B min'},
    'E min': {'Time' : 13, 'Same' : 'E min', 'Left' : 'A min', 'Right' : 'B min', 'Vertical' : 'G maj', 'Jump' : 'F# min'},
    'B min': {'Time' : 14, 'Same' : 'B min', 'Left' : 'E min', 'Right' : 'F# min', 'Vertical' : 'D maj', 'Jump' : 'C# min'},
    'F# min': {'Time' : 15, 'Same' : 'F# min', 'Left' : 'B min', 'Right' : 'C# min', 'Vertical' : 'A maj', 'Jump' : 'G# min'},
    'C# min': {'Time' : 16, 'Same' : 'C# min', 'Left' : 'F# min', 'Right' : 'G# min', 'Vertical' : 'E maj', 'Jump' : 'D# min'},
    'G# min': {'Time' : 17, 'Same' : 'G# min', 'Left' : 'C# min', 'Right' : 'D# min', 'Vertical' : 'B maj', 'Jump' : 'A# min'},
    'D# min': {'Time' : 18, 'Same' : 'D# min', 'Left' : 'G# min', 'Right' : 'A# min', 'Vertical' : 'F# maj', 'Jump' : 'F min'},
    'A# min': {'Time' : 19, 'Same' : 'A# min', 'Left' : 'D# min', 'Right' : 'F min', 'Vertical' : 'C# maj', 'Jump' : 'C min'},
    'F min': {'Time' : 20, 'Same' : 'F min', 'Left' : 'A# min', 'Right' : 'C min', 'Vertical' : 'G# maj', 'Jump' : 'G min'},
    'C min': {'Time' : 21, 'Same' : 'C min', 'Left' : 'F min', 'Right' : 'G min', 'Vertical' : 'D# maj', 'Jump' : 'D min'},
    'G min': {'Time' : 22, 'Same' : 'G min', 'Left' : 'C min', 'Right' : 'D min', 'Vertical' : 'A# maj', 'Jump' : 'A min'},
    'D min': {'Time' : 23, 'Same' : 'D min', 'Left' : 'G min', 'Right' : 'A min', 'Vertical' : 'F maj', 'Jump' : 'E min'}}
    initkey = np.random.choice(key_time_conv.keys())
    nextkey = ['Same', 'Left', 'Right', 'Vertical', 'Jump']
    probabilities = [0.45, 0.15, 0.15, 0.15, .1]
    counter = 2
    keylist = [initkey]
    df = inkey(catalog,1,initkey)
    while counter < length:
        new_key = np.random.choice(nextkey, p=probabilities)
        new_key = key_time_conv.get(keylist[-1]).get(new_key)
        keylist.append(new_key)
        counter +=1
    for x in keylist:
        df = df.append(inkey(catalog,1,x))
    df['time_sig'] = df['Key'].apply(lambda x : key_time_conv.get(x).get('Time'))
    return df

key_list = ['C','C#','D','D#','E','F','F#','G','G#','A','A#','B']
key_list_full = key_list + key_list
key_dict = dict(zip(key_list, range(0,12)))

x = 0
initkey = np.random.choice(key_time_conv.keys())
nextkey = ['Same', 'Left', 'Right', 'Vertical', 'Jump']
probabilities = [0.45, 0.15, 0.15, 0.15, .1]

class Scale:
    def __init__(self, start_key):
        self.start_key = start_key
    def major_scale(self):
        x = key_dict.get(self.start_key)
        major = [key_list_full[x],key_list_full[x+2],key_list_full[x+4],key_list_full[x+5],
        key_list_full[x+7],key_list_full[x+9],key_list_full[x+11]]
        return major
    def minor_scale(self):
        x = key_dict.get(self.start_key)
        minor = [key_list_full[x],key_list_full[x+2],key_list_full[x+3],key_list_full[x+5],
        key_list_full[x+7],key_list_full[x+8],key_list_full[x+10]]
        return minor

    start = key_dict.get(start_key)

    def major_scale(self):

    def minor_scale(self):
