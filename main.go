package main

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"
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

func getClient(ctx context.Context, bucket string) (*s3.Client, error) {
	region, err := getRegion(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("getting region: %w", err)
	}

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("loading config: %w", err)
	}

	return s3.NewFromConfig(cfg), nil
}

func upload(ctx context.Context, bucket, key string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	client, err := getClient(ctx, bucket)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	output, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}

	wg, wgctx := errgroup.WithContext(ctx)
	wg.SetLimit(connections)

	var mu sync.Mutex
	var completed []types.CompletedPart

	upload := func(partNum int32, chunk *chunk) func() error {
		return func() error {
			defer chunkpool.Put(chunk)

			resp, err := client.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:     aws.String(bucket),
				Key:        aws.String(key),
				UploadId:   output.UploadId,
				PartNumber: aws.Int32(partNum),
				Body:       bytes.NewReader(chunk.buf[:chunk.n]),
			})
			if err != nil {
				return err
			}

			mu.Lock()
			completed = append(completed, types.CompletedPart{
				ETag:       resp.ETag,
				PartNumber: &partNum,
			})
			mu.Unlock()

			return nil
		}
	}

	partNum := int32(1)
	for {
		if wgctx.Err() != nil {
			break
		}

		chk := chunkpool.Get().(*chunk)
		chk.n, chk.err = io.ReadFull(os.Stdin, chk.buf)
		eof := errors.Is(chk.err, io.EOF) || errors.Is(chk.err, io.ErrUnexpectedEOF)
		if chk.n == 0 && eof {
			break
		}
		if eof {
			chk.err = nil
		}
		if chk.err != nil {
			return chk.err
		}

		wg.Go(upload(partNum, chk))

		partNum++
	}

	if err := wg.Wait(); err != nil {
		return err
	}

	slices.SortFunc(completed, func(a, b types.CompletedPart) int {
		return cmp.Compare(*a.PartNumber, *b.PartNumber)
	})

	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: output.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completed,
		},
	})

	return err
}

func download(ctx context.Context, bucket, key string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	client, err := getClient(ctx, bucket)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

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

	stat, _ := os.Stdin.Stat()
	path := strings.TrimPrefix(u.Path, "/")

	if stat.Mode()&os.ModeCharDevice == 0 {
		if err := upload(context.Background(), u.Host, path); err != nil {
			panic(err)
		}
	} else {
		if err := download(context.Background(), u.Host, path); err != nil {
			panic(err)
		}
	}
}
