package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	chunksize   int64 = 16 * 1024 * 1024
	connections int   = 32
)

var chunkpool = sync.Pool{
	New: func() any {
		return &chunk{buf: make([]byte, chunksize)}
	},
}

type chunk struct {
	buf []byte
	n   int
	err error
}

func getRegion(ctx context.Context, bucket string) (string, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", fmt.Errorf("loading config: %w", err)
	}

	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}

	client := s3.NewFromConfig(cfg)

	location, err := client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: aws.String(bucket)})
	if err != nil {
		return "", fmt.Errorf("bucket location: %w", err)
	}

	if location.LocationConstraint == "" {
		return "us-east-1", nil
	}

	return string(location.LocationConstraint), nil
}

func download(ctx context.Context, bucket, key string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	region, err := getRegion(ctx, bucket)
	if err != nil {
		return fmt.Errorf("getting region: %w", err)
	}

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("head object: %w", err)
	}

	ordered := make(chan chan *chunk, connections)
	total := aws.ToInt64(head.ContentLength)

	go func() {
		defer close(ordered)

		for off := int64(0); off < total; {
			if ctx.Err() != nil {
				break
			}

			end := min(off+chunksize, total) - 1

			item := make(chan *chunk, 1)
			ordered <- item
			chk := chunkpool.Get().(*chunk)
			chk.n = 0
			chk.err = nil

			go func(off, end int64, chunk *chunk) {
				object, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
					Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, end)),
				})

				if err != nil {
					chunk.err = err
				} else {
					chunk.n, chunk.err = io.ReadAtLeast(object.Body, chunk.buf, int(end-off+1))
					object.Body.Close()
				}

				item <- chunk
			}(off, end, chk)

			off = end + 1
		}
	}()

	for item := range ordered {
		chunk := <-item

		_, err := os.Stdout.Write(chunk.buf[:chunk.n])
		if err != nil {
			return err
		}
		if chunk.err != nil {
			return chunk.err
		}

		chunkpool.Put(chunk)
	}

	return nil
}

func main() {
	flag.Int64Var(&chunksize, "chunksize", 16*1024*1024, "chunk size")
	flag.IntVar(&connections, "connections", 32, "connections")
	flag.Parse()

	if len(flag.Args()) < 1 {
		fmt.Fprintf(os.Stderr, "usage: %s s3://bucket/key\n", os.Args[0])
		return
	}

	u, err := url.Parse(flag.Arg(0))
	if err != nil {
		panic(err)
	}

	if err := download(context.Background(), u.Host, strings.TrimPrefix(u.Path, "/")); err != nil {
		panic(err)
	}
}
