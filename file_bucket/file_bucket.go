package file_bucket

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/zeroxsolutions/alex"
	"github.com/zeroxsolutions/barbatos/bucket"
	"github.com/zeroxsolutions/narrative/utils"
)

type FileBucket struct {
	BasePath string
	mu       sync.Mutex
	locks    map[string]*sync.Mutex
}

func NewFileBucket(config alex.FileBucketConfig) (bucket.Bucket, error) {
	if err := os.MkdirAll(config.BasePath, 0o755); err != nil {
		return nil, err
	}
	return &FileBucket{
		BasePath: config.BasePath,
		locks:    make(map[string]*sync.Mutex),
	}, nil
}

func (b *FileBucket) lockFor(objectName string) func() {
	b.mu.Lock()
	lk, ok := b.locks[objectName]
	if !ok {
		lk = &sync.Mutex{}
		b.locks[objectName] = lk
	}
	b.mu.Unlock()

	lk.Lock()
	return func() { lk.Unlock() }
}

func (b *FileBucket) PutObject(ctx context.Context, objectName string, reader io.Reader, _ int64) error {
	unlock := b.lockFor(objectName)
	defer unlock()
	fullPath := filepath.Join(b.BasePath, objectName)
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmpFile, err := os.CreateTemp(dir, filepath.Base(fullPath)+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmpFile.Name()
	cleanup := true
	defer func() {
		_ = tmpFile.Close()
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := io.Copy(tmpFile, reader); err != nil {
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}

	if err := os.Rename(tmpName, fullPath); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func (b *FileBucket) GetObject(ctx context.Context, objectName string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fullPath, err := secureJoin(b.BasePath, objectName)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, bucket.ErrNotFound
		}
		return nil, err
	}
	return f, nil
}

func (b *FileBucket) Stats(ctx context.Context, objectName string) (*bucket.Stats, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if objectName == "" || strings.ContainsRune(objectName, '\x00') {
		return nil, fmt.Errorf("invalid object name")
	}

	fullPath, err := secureJoin(b.BasePath, objectName)
	if err != nil {
		return nil, err
	}

	realPath, err := filepath.EvalSymlinks(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, bucket.ErrNotFound
		}
		return nil, err
	}
	if !isSubpath(b.BasePath, realPath) {
		return nil, fmt.Errorf("path escapes base directory")
	}

	st, err := os.Stat(realPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, bucket.ErrNotFound
		}
		return nil, err
	}
	if st.IsDir() {
		// Return not found for directory to handle 404
		return nil, bucket.ErrNotFound
	}

	f, err := os.Open(realPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	ct := utils.DetectContentType(data, objectName)

	return &bucket.Stats{
		Size:         st.Size(),
		ContentType:  ct,
		LastModified: st.ModTime().UTC(),
	}, nil
}

// ---- helpers ----

func secureJoin(base, name string) (string, error) {
	// Block absolute path
	if filepath.IsAbs(name) {
		return "", fmt.Errorf("absolute path not allowed")
	}
	clean := filepath.Clean(name)
	// Block ".." escape base
	if clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("path escapes base directory")
	}
	full := filepath.Join(base, clean)

	baseAbs, _ := filepath.Abs(base)
	fullAbs, _ := filepath.Abs(full)
	rel, err := filepath.Rel(baseAbs, fullAbs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("path escapes base directory")
	}
	return fullAbs, nil
}

func isSubpath(base, target string) bool {
	baseAbs, _ := filepath.Abs(base)
	tgtAbs, _ := filepath.Abs(target)
	rel, err := filepath.Rel(baseAbs, tgtAbs)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}
