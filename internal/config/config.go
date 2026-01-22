package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	Headers      map[string]string `json:"headers"`
	Aria2RPCUrl  string            `json:"aria2_rpc_url"`
	Aria2Secret  string            `json:"aria2_secret"`
	ProxyPort    int               `json:"proxy_port"`
	CacheDir     string            `json:"cache_dir"`
}

var GlobalConfig = Config{
	Headers: map[string]string{
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	},
	Aria2RPCUrl: "http://localhost:6800/jsonrpc",
	ProxyPort:   8084,
	CacheDir:    "./cache",
}

func LoadConfig(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Use defaults
		}
		return err
	}
	return json.Unmarshal(data, &GlobalConfig)
}
