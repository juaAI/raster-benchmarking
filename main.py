import os
import time
import random

import psutil
import h5py
import zarr
import xarray

data_path = "data"
files = ["2017.h5", "2017.zarr", "compressed_2017.h5"]
data = [os.path.join(data_path, i) for i in files] 

def get_ram_usage():
    print(f"Current RAM usage: {psutil.virtual_memory()[2]} GB") 

def generate_random_list(num):
    rand_list = [random.randint(0,1460) for i in range(num)]
    return rand_list

def get_h5_dataset():
    f = h5py.File(data[0])
    dset = f["fields"]
    return dset

def save_compressed_h5(dset):

    f = h5py.File("data/compressed_2017.h5", "w")
    f.create_dataset("fields", data=dset[()], compression="gzip")
    f.close()

def get_zarr_dataset(engine="zarr"):
    if engine == "zarr":
        f = zarr.open(data[1])
    elif engine == "xarray":
        f = xarray.open_zarr(data[1])
    return f
    
def test_zarr_full(f):
    keys = list(dict(f).keys())
    needed_keys = keys[:-3]

    for i in needed_keys:
        print(i)
        f[i][:100]

def test_xarray_iter(f, num):

    for i in range(num):
        ds = f.isel(time=i).load()
        print(ds)
        
def test_xarray_slice(f, num):

    ds = f.isel(time=list(range(num))).load()


def test_iter(dset, num):
    for i in range(num):
        test = dset[i]
    get_ram_usage()

def test_slice(dset, num):
    test = dset[:num]
    get_ram_usage()

def test_random_access(dset, num):
    rand_list = generate_random_list(num)
    for i in rand_list:
        test = dset[i]
    get_ram_usage()

def test_full_file_iter(dset):
    for i in range(1460):
        test = dset[i]
    get_ram_usage()

def test_full_file_slice(dset):
    test = dset[:]
    get_ram_usage()

num = 100


def test_h5():
    get_ram_usage()
    print(f"Opening dataset")

    dset = get_dataset()
    get_ram_usage()

    start = time.time()
    test_iter(dset, num)
    print(f"Took {time.time() - start} seconds to access {num} datapoints via iteration")

    start = time.time()
    test_slice(dset, num)
    print(f"Took {time.time() - start} seconds to access {num} datapoints via slice")

    start = time.time()
    test_random_access(dset, num)
    print(f"Took {time.time() - start} seconds to randomly access {num} datapoints via iteration")

    start = time.time()
    test_full_file_iter(dset)
    print(f"Took {time.time() - start} seconds to access full dataset of 1460 timesteps via iteration")

    start = time.time()
    test_full_file_slice(dset)
    print(f"Took {time.time() - start} seconds to access full dataset of 1460 timesteps via slice")


def test_zarr():
    f = get_zarr_dataset(engine="xarray")
    test_xarray_slice(f, 100)

start = time.time()
test_zarr()
print(f"Took {time.time() - start} seconds")

#dset = get_h5_dataset()
#save_compressed_h5(dset)