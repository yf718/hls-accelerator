package m3u8

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"path/filepath"

	"github.com/grafov/m3u8"
)

type PlaylistType int

const (
	Master PlaylistType = iota
	Variant
	Unknown
)

type DownloadItem struct {
	URL      string
	Filename string
	Type     string // "ts" or "key"
}

// Parse checks the content and returns the type and parsed object
func Parse(content io.Reader) (m3u8.Playlist, PlaylistType, error) {
	p, listType, err := m3u8.DecodeFrom(content, true)
	if err != nil {
		return nil, Unknown, err
	}

	switch listType {
	case m3u8.MASTER:
		return p, Master, nil
	case m3u8.MEDIA:
		return p, Variant, nil
	default:
		return nil, Unknown, fmt.Errorf("unknown playlist type")
	}
}

// ResolveURL resolves a relative reference against a base URL
func ResolveURL(base *url.URL, ref string) string {
	return resolveURL(base, ref)
}

// resolveURL resolves a relative reference against a base URL
func resolveURL(base *url.URL, ref string) string {
	refURL, err := url.Parse(ref)
	if err != nil {
		return ref // fallback
	}
	return base.ResolveReference(refURL).String()
}

// RewriteMaster rewrites URIs in a master playlist
func RewriteMaster(p *m3u8.MasterPlaylist, proxyBaseURL string, originBaseURL *url.URL) string {
	for _, v := range p.Variants {
		fullURL := resolveURL(originBaseURL, v.URI)
		encodedURL := url.QueryEscape(fullURL)
		v.URI = fmt.Sprintf("%s/m3u8/%s", proxyBaseURL, encodedURL)
	}
	return p.String()
}

// RewriteVariant rewrites URIs in a media playlist and returns download plan
// proxyBaseURL: http://localhost:PORT/proxy
// taskID: unique ID for cache
func RewriteVariant(p *m3u8.MediaPlaylist, proxyBaseURL, taskID string, originBaseURL *url.URL) (string, []DownloadItem, int) {
	items := []DownloadItem{}
	seenKeys := make(map[string]bool)
	totalSegments := 0

	// We iterate through segments to collect URLs and rewrite them
	for i, seg := range p.Segments {
		if seg == nil {
			continue
		}

		if seg.URI != "" {
			totalSegments++
		}

		// Resolve and Rewrite Segment URI
		if seg.URI != "" {
			fullSegURL := resolveURL(originBaseURL, seg.URI)

			// Determine extension
			ext := ".ts"
			if u, err := url.Parse(fullSegURL); err == nil {
				if e := filepath.Ext(u.Path); e != "" {
					ext = e
				}
			}

			filename := fmt.Sprintf("%05d%s", i+1, ext) // 1-based index with ext
			encodedURL := url.QueryEscape(fullSegURL)

			// Rewrite to: /proxy/seg/{taskID}/{filename}/{encoded_url}
			seg.URI = fmt.Sprintf("%s/seg/%s/%s/%s", proxyBaseURL, taskID, filename, encodedURL)

			items = append(items, DownloadItem{
				URL:      fullSegURL,
				Filename: filename,
				Type:     "ts",
			})
		}

		// Resolve and Rewrite Key URI
		if seg.Key != nil && seg.Key.URI != "" {
			fullKeyURL := resolveURL(originBaseURL, seg.Key.URI)

			// Filename: md5(url).key
			hash := md5.Sum([]byte(fullKeyURL))
			filename := hex.EncodeToString(hash[:]) + ".key"
			encodedURL := url.QueryEscape(fullKeyURL)

			// Rewrite to: /proxy/key/{taskID}/{filename}/{encoded_url}
			seg.Key.URI = fmt.Sprintf("%s/key/%s/%s/%s", proxyBaseURL, taskID, filename, encodedURL)

			if !seenKeys[filename] {
				items = append(items, DownloadItem{
					URL:      fullKeyURL,
					Filename: filename,
					Type:     "key",
				})
				seenKeys[filename] = true
			}
		}
	}
	return p.String(), items, totalSegments
}
