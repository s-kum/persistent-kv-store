# persistent-key-value-store

This is an attempt to create a disk based key value store, disk being used as a back up copy of the values, the data will stay in memory. 

The Store is thread-safe, please see the junits for potential usage. THe store supports get(), put(), remove(), clear() apis.
