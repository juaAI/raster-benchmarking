import os
import time

import psutil
import h5py

data_path = "data"
files = ["2017.h5", "2017.zarr"]
data = [os.path.join(data_path, i) for i in files] 

def get_ram_usage():
    print(f"Current RAM usage: {psutil.virtual_memory()[2]} GB") 

def get_dataset(index):
    f = h5py.File(data[index])
    dset = f["fields"]
    return dset

def test_iter(dset, num):
    for i in range(num):
        test = dset[i]
    get_ram_usage()

def test_slice(dset, num):
    test = dset[:num]
    get_ram_usage()

def test_full_file(dset):
    for i in range(1460):
        test = dset[i]
    get_ram_usage()

start = time.time()
num = 100

get_ram_usage()
print(f"Opening dataset")

dset = get_dataset(0)
get_ram_usage()

test_iter(dset, num)
print(f"Took {time.time() - start} seconds to access {num} datapoints via iteration")

test_slice(dset, num)
print(f"Took {time.time() - start} seconds to access {num} datapoints via iteration")

test_full_file(dset)
print(f"Took {time.time() - start} seconds to access full dataset of 1460 timesteps")
