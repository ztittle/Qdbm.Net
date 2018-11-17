# Qdbm.Net
.NET port of the [QDBM Embedded Database](https://fallabs.com/qdbm/)

# Why?

The QDBM Database works quite well for embedded systems that due not support multithreading. However, with the POSIX API, it only supports file access and I need to manage a QDBM database in-memory (any Stream).
