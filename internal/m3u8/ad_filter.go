package m3u8

import (
	"net/url"
	"strings"

	"github.com/grafov/m3u8"
)

// AdFilter 定义广告过滤器的接口
// 使用策略模式，方便扩展不同的广告过滤规则
type AdFilter interface {
	// Match 检查给定的URL是否匹配此过滤器
	Match(originURL *url.URL) bool

	// Filter 过滤掉广告片段，返回应该保留的片段索引集合
	// segments: 所有片段
	// 返回: 应该保留的片段索引集合（map[索引]bool）
	Filter(segments []*m3u8.MediaSegment) map[int]bool
}

// AdFilterRegistry 广告过滤器注册表
// 使用注册表模式，方便管理和扩展
type AdFilterRegistry struct {
	filters []AdFilter
}

// NewAdFilterRegistry 创建新的广告过滤器注册表
func NewAdFilterRegistry() *AdFilterRegistry {
	registry := &AdFilterRegistry{
		filters: []AdFilter{},
	}
	// 注册默认的过滤器
	registry.Register(&FFZYAdFilter{})
	return registry
}

// Register 注册一个新的广告过滤器
func (r *AdFilterRegistry) Register(filter AdFilter) {
	r.filters = append(r.filters, filter)
}

// GetFilter 根据URL获取匹配的过滤器
// 返回第一个匹配的过滤器，如果没有匹配的则返回nil
func (r *AdFilterRegistry) GetFilter(originURL *url.URL) AdFilter {
	for _, filter := range r.filters {
		if filter.Match(originURL) {
			return filter
		}
	}
	return nil
}

// FFZYAdFilter FFZY网站的广告过滤器
// 规则：在两个 #EXT-X-DISCONTINUITY 之间正好有5个片段，则这些片段是广告
type FFZYAdFilter struct{}

// Match 检查URL是否包含ffzy
func (f *FFZYAdFilter) Match(originURL *url.URL) bool {
	return strings.Contains(originURL.Host, "ffzy") || strings.Contains(originURL.String(), "ffzy")
}

// Filter 过滤广告片段
func (f *FFZYAdFilter) Filter(segments []*m3u8.MediaSegment) map[int]bool {
	keep := make(map[int]bool)
	if len(segments) == 0 {
		return keep
	}

	// 记录每个片段是否有 DISCONTINUITY 标记
	discontinuityIndices := []int{}
	for i, seg := range segments {
		if seg != nil && seg.Discontinuity {
			discontinuityIndices = append(discontinuityIndices, i)
		}
	}

	// 如果没有 DISCONTINUITY，保留所有片段
	if len(discontinuityIndices) == 0 {
		for i := range segments {
			if segments[i] != nil && segments[i].URI != "" {
				keep[i] = true
			}
		}
		return keep
	}

	// 标记所有片段为保留
	for i := range segments {
		if segments[i] != nil && segments[i].URI != "" {
			keep[i] = true
		}
	}

	// 检查每对连续的 DISCONTINUITY 之间的片段数量
	for i := 0; i < len(discontinuityIndices)-1; i++ {
		start := discontinuityIndices[i]
		end := discontinuityIndices[i+1]

		// 计算两个 DISCONTINUITY 之间的有效片段数量（不包括 DISCONTINUITY 本身）
		segmentCount := 0
		for j := start + 1; j < end; j++ {
			if segments[j] != nil && segments[j].URI != "" {
				segmentCount++
			}
		}

		// 如果正好是5个片段，则标记为广告（移除）
		if segmentCount == 5 {
			for j := start + 1; j < end; j++ {
				if segments[j] != nil && segments[j].URI != "" {
					delete(keep, j)
				}
			}
		}
	}

	return keep
}

// 全局注册表实例
var defaultRegistry *AdFilterRegistry

// init 初始化默认注册表
func init() {
	defaultRegistry = NewAdFilterRegistry()
}

// GetAdFilter 获取匹配的广告过滤器
func GetAdFilter(originURL *url.URL) AdFilter {
	return defaultRegistry.GetFilter(originURL)
}

// RegisterAdFilter 注册新的广告过滤器（供外部扩展使用）
func RegisterAdFilter(filter AdFilter) {
	defaultRegistry.Register(filter)
}
