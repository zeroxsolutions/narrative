package minio

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/zeroxsolutions/alex"
	"github.com/zeroxsolutions/barbatos/bucket"
)

type MinioBucket struct {
	Client     *minio.Client
	BucketName string
}

func NewMinioBucket(config alex.MinioConfig) (bucket.Bucket, error) {
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKey, config.SecretKey, ""),
		Secure: config.UseSSL,
	})
	if err != nil {
		return nil, err
	}
	exists, err := client.BucketExists(context.TODO(), config.BucketName)
	if err != nil {
		return nil, err
	}
	if !exists {
		err = client.MakeBucket(context.TODO(), config.BucketName, minio.MakeBucketOptions{
			Region: config.Region,
		})
		if err != nil {
			return nil, err
		}
	}
	return &MinioBucket{
		Client:     client,
		BucketName: config.BucketName,
	}, nil
}

func (minioBucket *MinioBucket) PutObject(ctx context.Context, objectName string, reader io.Reader, readerLen int64) error {
	_, err := minioBucket.Client.PutObject(ctx, minioBucket.BucketName, objectName, reader, readerLen, minio.PutObjectOptions{})
	return err
}

func (minioBucket *MinioBucket) GetObject(ctx context.Context, objectName string) (io.ReadCloser, error) {
	return minioBucket.Client.GetObject(ctx, minioBucket.BucketName, objectName, minio.GetObjectOptions{})
}

func (m *MinioBucket) Stats(ctx context.Context, objectName string) (*bucket.Stats, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	info, err := m.Client.StatObject(ctx, m.BucketName, objectName, minio.StatObjectOptions{})
	if err != nil {
		er := minio.ToErrorResponse(err)
		if er.StatusCode == http.StatusNotFound || er.Code == "NoSuchKey" || er.Code == "NotFound" {
			return nil, bucket.ErrNotFound
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, err
	}
	ct := info.ContentType
	if ct == "" {
		ct = mime.TypeByExtension(strings.ToLower(filepath.Ext(objectName)))
		if ct == "" {
			ct = "application/octet-stream"
		}
	}
	return &bucket.Stats{
		Size:         info.Size,
		ContentType:  ct,
		LastModified: info.LastModified.UTC(),
	}, nil
}
