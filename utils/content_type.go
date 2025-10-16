package utils

import (
	"bytes"
	"math"
	"mime"
	"net/http"
	"path/filepath"
	"strings"
)

// DetectContentType infers the mime type from file extension and data bytes.
// priority: by extension -> sniff up to 512 bytes -> common image fallbacks -> octet-stream.
func DetectContentType(data []byte, objectName string) string {
	ext := strings.ToLower(filepath.Ext(objectName))

	// try by file extension first
	if ext != "" {
		if ct := mime.TypeByExtension(ext); ct != "" {
			return normalizeImageCT(ct, ext)
		}
	}

	// sniff up to 512 bytes from the buffer
	n := 512
	if len(data) < n {
		n = len(data)
	}
	if n > 0 {
		ct := http.DetectContentType(data[:n])
		if ct != "" && ct != "application/octet-stream" {
			return normalizeImageCT(ct, ext)
		}
	}

	// lightweight svg heuristic if mime db fails
	lower := bytes.ToLower(data)
	if len(lower) > 0 && bytes.Contains(lower[:int(math.Min(float64(len(lower)), 1024.0))], []byte("<svg")) {
		return "image/svg+xml"
	}

	// common image fallbacks by extension
	switch ext {
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".webp":
		return "image/webp"
	case ".gif":
		return "image/gif"
	case ".avif":
		return "image/avif"
	case ".svg":
		return "image/svg+xml"
	case ".ico":
		return "image/x-icon"
	}

	// safest default if everything else fails
	return "application/octet-stream"
}

// normalizeImageCT fixes a few non-standard variants and aligns svg types.
func normalizeImageCT(ct, ext string) string {
	// map image/jpg to the standard image/jpeg
	if ct == "image/jpg" {
		return "image/jpeg"
	}
	// some sniffers return text/xml for svg: prefer proper svg mime when extension is .svg
	if strings.HasPrefix(ct, "text/xml") && ext == ".svg" {
		return "image/svg+xml"
	}
	return ct
}
