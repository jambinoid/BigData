import argparse
import os
import random
import struct
import time

import mmap
import multiprocessing

from tqdm import tqdm

            
def _chunked(mmaped_f: mmap.mmap):
    chunk_size = mmaped_f.size() // os.cpu_count()
    intervals = list(range(0, mmaped_f.size(), chunk_size)) + [None]
    for start, end in tqdm(zip(intervals[:-1], intervals[1:])):
        yield mmaped_f[start:end]
        
        
def _read_chunk(chunk: mmap.mmap) -> tuple:
    iterator = struct.iter_unpack('>I', chunk)
    
    sum = 0
    min = 4294967295
    max = 0
    
    for (i,) in tqdm(iterator):
        sum += i
        if i < min:
            min = i
        if i > max:
            max = i
        
    return sum, min, max


def _read_chunked(filepath: str) -> tuple:
    with open(filepath, 'rb') as f:
        with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mmaped_f:
            pool = multiprocessing.Pool()
            results = pool.map(_read_chunk, _chunked(mmaped_f))
        
    sums, mins, maxs = tuple(map(list, zip(*results)))
        
    return sum(sums), min(mins), max(maxs)
    

def _read_straight(filepath: str) -> tuple:
    with open(filepath, 'rb') as f:
        iterator = struct.iter_unpack('>I', f.read())
        
    sum = 0
    min = 4294967295
    max = 0
    
    for (i,) in tqdm(iterator):
        sum += i
        if i < min:
            min = i
        if i > max:
            max = i
        
    return sum, min, max


def read_n_calc(filepath: str, use_multiprocessing: bool) -> tuple:
    if use_multiprocessing:
        return _read_chunked(filepath)
    else:
        return _read_straight(filepath)
        