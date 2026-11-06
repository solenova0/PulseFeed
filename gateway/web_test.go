package main

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestEmbeddedAssetsRevalidateAndSupportHEAD(t *testing.T) {
	handler := newAssetHandler()
	for path, contentType := range map[string]string{
		"/": "text/html", "/app.js": "text/javascript", "/styles.css": "text/css",
	} {
		t.Run(path, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
			if response.Code != http.StatusOK || response.Body.Len() == 0 || !strings.HasPrefix(response.Header().Get("Content-Type"), contentType) {
				t.Fatalf("status=%d type=%q bytes=%d", response.Code, response.Header().Get("Content-Type"), response.Body.Len())
			}
			etag := response.Header().Get("ETag")
			if etag == "" || response.Header().Get("Cache-Control") != "no-cache" {
				t.Fatal("asset missing cache revalidation headers")
			}
			if response.Header().Get("X-Content-Type-Options") != "nosniff" || response.Header().Get("X-Frame-Options") != "DENY" || !strings.Contains(response.Header().Get("Content-Security-Policy"), "script-src 'self'") {
				t.Fatal("asset missing browser security headers")
			}
			request := httptest.NewRequest(http.MethodGet, path, nil)
			request.Header.Set("If-None-Match", etag)
			revalidated := httptest.NewRecorder()
			handler.ServeHTTP(revalidated, request)
			if revalidated.Code != http.StatusNotModified || revalidated.Body.Len() != 0 {
				t.Fatal("unchanged asset was not revalidated")
			}
			head := httptest.NewRecorder()
			handler.ServeHTTP(head, httptest.NewRequest(http.MethodHead, path, nil))
			if head.Code != http.StatusOK || head.Body.Len() != 0 || head.Header().Get("Content-Length") != strconv.Itoa(response.Body.Len()) || head.Header().Get("ETag") != etag {
				t.Fatal("HEAD differs from the corresponding GET")
			}
		})
	}
}

func TestAssetsDoNotExposeFilesOrShadowAPIRoutes(t *testing.T) {
	a, err := newApplication(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer a.redis.Close()
	handler := a.routes()
	for _, path := range []string{"/missing", "/web/", "/web/index.html", "/main.go", "/submit", "/cancel", "/ws/"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusNotFound {
			t.Fatalf("%s status=%d", path, response.Code)
		}
	}
	for path, want := range map[string]int{
		"/healthz": http.StatusOK, "/readyz": http.StatusServiceUnavailable,
		"/metrics": http.StatusOK, "/ws": http.StatusServiceUnavailable,
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != want || response.Header().Get("ETag") != "" {
			t.Fatalf("API route %s shadowed: status=%d", path, response.Code)
		}
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/", nil))
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != "GET, HEAD" {
		t.Fatal("asset route accepted an unsupported method")
	}
}

func TestHTTPAddressUsesCloudPortAndExplicitOverride(t *testing.T) {
	t.Setenv("HTTP_ADDR", "")
	t.Setenv("PORT", "10000")
	if address, err := httpAddress(); err != nil || address != ":10000" {
		t.Fatalf("address=%q error=%v", address, err)
	}
	for _, port := range []string{"zero", "0", "65536", "-1"} {
		t.Setenv("PORT", port)
		if _, err := httpAddress(); err == nil {
			t.Fatalf("invalid PORT %q accepted", port)
		}
	}
	t.Setenv("HTTP_ADDR", "127.0.0.1:9090")
	if address, err := httpAddress(); err != nil || address != "127.0.0.1:9090" {
		t.Fatalf("explicit address=%q error=%v", address, err)
	}
	t.Setenv("HTTP_ADDR", "")
	t.Setenv("PORT", "")
	if address, err := httpAddress(); err != nil || address != ":8080" {
		t.Fatalf("default address=%q error=%v", address, err)
	}
}
