# kneeslap

`kneeslap` downloads an S3 object in chunks concurrently and outputs the data
in order to `stdout`.

## why

Using `aws s3 cp` to `stdout` was slower than when writing to disk, presumably
because when writing to a file, the download is chunked and parts written
concurrently. I could not find a way to achieve similar performance to `stdout` 
(even when changing `max_concurrent_requests` and `multipart_chunksize`). If
you know of a setting that enables this, making `kneeslap` redundant, please
let me know!

## usage

`kneeslap s3://bucket/key` will download and output to `stdout`.

- You can use `pv` to see performance: `kneeslap s3://bucket/key | pv -X`.
- The `-connections` flag sets how many connections are made to the object, default is 32.
- The `-chunksize` flag sets the chunk size, default is 16MB.

## example

A good example of when to use this is if you're doing on-the-fly (de)compression with `zstd`.

For example, you could upload an object with:

```shell
tar -cvf - file1 file2 | zstd --sparse -c -3 -T0 | aws s3 cp - s3://bucket/key.zstd
```

And download it with:

```shell
kneeslap s3://bucket/key | zstd -d | tar -Szxf -
```

This would be comparable to `aws s3 cp s3://bucket/key - | zstd -d | tar -Szxf`, but there _should_ be a pretty significant speed increase (at least on very large files...)
