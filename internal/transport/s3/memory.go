package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Memory is an in-process S3 API for tests.
type Memory struct {
	mu   sync.Mutex
	data map[string]memObj
}

type memObj struct {
	body     []byte
	meta     map[string]string
	modified time.Time
}

func NewMemory() *Memory {
	return &Memory{data: make(map[string]memObj)}
}

func (m *Memory) key(bucket, key string) string { return bucket + "\x00" + key }

func (m *Memory) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	prefix := aws.ToString(params.Prefix)
	bucket := aws.ToString(params.Bucket)
	var contents []types.Object
	for k, v := range m.data {
		parts := strings.SplitN(k, "\x00", 2)
		if len(parts) != 2 || parts[0] != bucket {
			continue
		}
		if prefix != "" && !strings.HasPrefix(parts[1], prefix) {
			continue
		}
		sz := int64(len(v.body))
		mt := v.modified
		contents = append(contents, types.Object{Key: aws.String(parts[1]), Size: &sz, LastModified: &mt})
	}
	return &s3.ListObjectsV2Output{Contents: contents, IsTruncated: aws.Bool(false)}, nil
}

func (m *Memory) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.data[m.key(aws.ToString(params.Bucket), aws.ToString(params.Key))]
	if !ok {
		return nil, fmt.Errorf("NotFound: %s", aws.ToString(params.Key))
	}
	sz := int64(len(o.body))
	mt := o.modified
	etag := fmt.Sprintf("\"%d-%x\"", sz, o.body)
	if len(o.body) > 16 {
		etag = fmt.Sprintf("\"%d-%x\"", sz, o.body[:16])
	}
	return &s3.HeadObjectOutput{ContentLength: &sz, LastModified: &mt, Metadata: o.meta, ETag: aws.String(etag)}, nil
}

func (m *Memory) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.data[m.key(aws.ToString(params.Bucket), aws.ToString(params.Key))]
	if !ok {
		return nil, fmt.Errorf("NotFound: %s", aws.ToString(params.Key))
	}
	body := o.body
	if r := aws.ToString(params.Range); r != "" {
		var start, end int64
		if _, err := fmt.Sscanf(r, "bytes=%d-%d", &start, &end); err == nil {
			if start < 0 || end >= int64(len(body)) || start > end {
				return nil, fmt.Errorf("invalid range")
			}
			body = body[start : end+1]
		}
	}
	sz := int64(len(body))
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: &sz,
		Metadata:      o.meta,
	}, nil
}

func (m *Memory) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var buf bytes.Buffer
	if params.Body != nil {
		if _, err := io.Copy(&buf, params.Body); err != nil {
			return nil, err
		}
	}
	meta := map[string]string{}
	for k, v := range params.Metadata {
		meta[k] = v
	}
	m.data[m.key(aws.ToString(params.Bucket), aws.ToString(params.Key))] = memObj{
		body: buf.Bytes(), meta: meta, modified: time.Now().UTC(),
	}
	return &s3.PutObjectOutput{}, nil
}

func (m *Memory) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, m.key(aws.ToString(params.Bucket), aws.ToString(params.Key)))
	return &s3.DeleteObjectOutput{}, nil
}

func (m *Memory) CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// CopySource is "bucket/key"
	src := aws.ToString(params.CopySource)
	idx := strings.Index(src, "/")
	if idx < 0 {
		return nil, fmt.Errorf("bad copy source")
	}
	srcBucket, srcKey := src[:idx], src[idx+1:]
	o, ok := m.data[m.key(srcBucket, srcKey)]
	if !ok {
		return nil, fmt.Errorf("NotFound")
	}
	cp := memObj{body: append([]byte(nil), o.body...), meta: map[string]string{}, modified: time.Now().UTC()}
	for k, v := range o.meta {
		cp.meta[k] = v
	}
	m.data[m.key(aws.ToString(params.Bucket), aws.ToString(params.Key))] = cp
	return &s3.CopyObjectOutput{}, nil
}
