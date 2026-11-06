package main

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"fmt"
	"net/http"
	"time"
)

//go:embed web/index.html web/app.js web/styles.css
var webContent embed.FS

func newAssetHandler() http.Handler {
	type asset struct {
		body        []byte
		contentType string
		etag        string
	}
	assets := make(map[string]asset)
	for path, contentType := range map[string]string{
		"/":           "text/html; charset=utf-8",
		"/app.js":     "text/javascript; charset=utf-8",
		"/styles.css": "text/css; charset=utf-8",
	} {
		filename := "web" + path
		if path == "/" {
			filename += "index.html"
		}
		body, err := webContent.ReadFile(filename)
		if err != nil {
			panic(err) // All three assets are required at compile time by go:embed.
		}
		assets[path] = asset{body: body, contentType: contentType, etag: fmt.Sprintf(`"%x"`, sha256.Sum256(body))}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		item, exists := assets[r.URL.Path]
		if !exists {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "GET or HEAD required", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", item.contentType)
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("ETag", item.etag)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self' ws: wss:; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
		http.ServeContent(w, r, r.URL.Path, time.Time{}, bytes.NewReader(item.body))
	})
}
