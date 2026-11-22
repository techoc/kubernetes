/*
Copyright 2015 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package env

import (
	"os"
	"strconv"
)

// GetEnvAsStringOrFallback 返回指定 key 的环境变量值；
// 未设置时回退到 defaultValue。
func GetEnvAsStringOrFallback(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

// GetEnvAsIntOrFallback 返回指定 key 的环境变量值并解析为整数；
// 未设置时回退到 defaultValue。解析失败时返回默认值和错误。
func GetEnvAsIntOrFallback(key string, defaultValue int) (int, error) {
	if v := os.Getenv(key); v != "" {
		value, err := strconv.Atoi(v)
		if err != nil {
			return defaultValue, err
		}
		return value, nil
	}
	return defaultValue, nil
}

// GetEnvAsFloat64OrFallback 返回指定 key 的环境变量值并解析为 float64；
// 未设置时回退到 defaultValue。解析失败时返回默认值和错误。
func GetEnvAsFloat64OrFallback(key string, defaultValue float64) (float64, error) {
	if v := os.Getenv(key); v != "" {
		value, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return defaultValue, err
		}
		return value, nil
	}
	return defaultValue, nil
}
