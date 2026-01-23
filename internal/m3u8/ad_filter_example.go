package m3u8

import (
	"net/url"
	"strings"

	"github.com/grafov/m3u8"
)

// 示例：如何添加新的广告过滤器
//
// 假设我们要为另一个网站（例如 example.com）添加广告过滤规则
// 规则：如果片段URL包含 "ad" 关键字，则认为是广告

// ExampleAdFilter 示例广告过滤器
type ExampleAdFilter struct{}

// Match 检查URL是否匹配
func (f *ExampleAdFilter) Match(originURL *url.URL) bool {
	return strings.Contains(originURL.Host, "example.com")
}

// Filter 过滤广告片段
func (f *ExampleAdFilter) Filter(segments []*m3u8.MediaSegment) map[int]bool {
	keep := make(map[int]bool)
	
	// 遍历所有片段，如果URL包含 "ad" 关键字，则过滤掉
	for i, seg := range segments {
		if seg == nil {
			continue
		}
		
		// 如果片段URI包含 "ad"，则认为是广告
		if strings.Contains(strings.ToLower(seg.URI), "ad") {
			// 不添加到 keep 中，即过滤掉
			continue
		}
		
		// 否则保留
		if seg.URI != "" {
			keep[i] = true
		}
	}
	
	return keep
}

// 使用示例：
// 在程序初始化时注册新的过滤器：
//   m3u8.RegisterAdFilter(&ExampleAdFilter{})
