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

// Package api 包含 kubeconfig 文件的内存表示，以及操作它们
// （合并、内联 flatten、脱敏 redact）的辅助函数。
// api 包定义 kubeconfig 的内存模型，以及合并、内联（flatten）、脱敏等辅助函数。
// 其类型与 v1 外部版本通过 conversion 相互转换。
package api

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

func init() {
	// 预解码占位字符串：ShortenConfig 依赖 json.Marshal 对 []byte 做 base64 编码的特性，
	// 先把想要展示的明文做一次 base64 解码，编码回去时即可还原为可读文本。
	sDec, _ := base64.StdEncoding.DecodeString("REDACTED+")
	redactedBytes = []byte(string(sDec))
	sDec, _ = base64.StdEncoding.DecodeString("DATA+OMITTED")
	dataOmittedBytes = []byte(string(sDec))
}

// IsConfigEmpty 在配置为空时返回 true。
func IsConfigEmpty(config *Config) bool {
	// 只有所有集合与当前上下文都为空才算空配置；Preferences 单独判断。
	return len(config.AuthInfos) == 0 && len(config.Clusters) == 0 && len(config.Contexts) == 0 &&
		len(config.CurrentContext) == 0 &&
		len(config.Preferences.Extensions) == 0 && !config.Preferences.Colors &&
		len(config.Extensions) == 0
}

// MinifyConfig 读取当前上下文，并据此只保留配置中相关的部分。
// 这有利于基于 kubeconfig 文件制作 Secret。
// MinifyConfig 裁剪配置，只保留 current-context 及其引用的 cluster/user，
// 以便把 kubeconfig 打包成更小的 Secret。引用的对象缺失时返回错误。
func MinifyConfig(config *Config) error {
	if len(config.CurrentContext) == 0 {
		return errors.New("current-context must exist in order to minify")
	}

	currContext, exists := config.Contexts[config.CurrentContext]
	if !exists {
		return fmt.Errorf("cannot locate context %v", config.CurrentContext)
	}

	newContexts := map[string]*Context{}
	newContexts[config.CurrentContext] = currContext

	newClusters := map[string]*Cluster{}
	if len(currContext.Cluster) > 0 {
		if _, exists := config.Clusters[currContext.Cluster]; !exists {
			return fmt.Errorf("cannot locate cluster %v", currContext.Cluster)
		}

		newClusters[currContext.Cluster] = config.Clusters[currContext.Cluster]
	}

	newAuthInfos := map[string]*AuthInfo{}
	if len(currContext.AuthInfo) > 0 {
		if _, exists := config.AuthInfos[currContext.AuthInfo]; !exists {
			return fmt.Errorf("cannot locate user %v", currContext.AuthInfo)
		}

		newAuthInfos[currContext.AuthInfo] = config.AuthInfos[currContext.AuthInfo]
	}

	// 用新映射整体替换，丢弃其余条目。
	config.AuthInfos = newAuthInfos
	config.Clusters = newClusters
	config.Contexts = newContexts

	return nil
}

var (
	// dataOmittedBytes 是写入 []byte 字段后经 JSON 编码呈 "DATA+OMITTED" 的占位值。
	dataOmittedBytes []byte
	// redactedBytes 是写入 []byte 字段后经 JSON 编码呈 "REDACTED+" 的占位值。
	redactedBytes []byte
)

// ShortenConfig 对配置对象中的原始数据条目做脱敏，便于以人类可读的方式查看。
// ShortenConfig 就地脱敏配置，用于人读展示：证书/密钥数据替换为 DATA+OMITTED，
// token 替换为 REDACTED。它直接修改传入对象，如需保留原始值请先深拷贝。
func ShortenConfig(config *Config) {
	// 先对我们想打印的内容做 base64 解码，骗过 json 编码器，让它在原始数据中
	// 打印出人类可读的字符串。这依赖于
	// http://golang.org/pkg/encoding/json/#Marshal 用 base64 编码 []byte 的实现。
	for key, authInfo := range config.AuthInfos {
		if len(authInfo.ClientKeyData) > 0 {
			authInfo.ClientKeyData = dataOmittedBytes
		}
		if len(authInfo.ClientCertificateData) > 0 {
			authInfo.ClientCertificateData = dataOmittedBytes
		}
		if len(authInfo.Token) > 0 {
			authInfo.Token = "REDACTED"
		}
		config.AuthInfos[key] = authInfo
	}
	for key, cluster := range config.Clusters {
		if len(cluster.CertificateAuthorityData) > 0 {
			cluster.CertificateAuthorityData = dataOmittedBytes
		}
		config.Clusters[key] = cluster
	}
}

// FlattenConfig 把配置对象变为自包含的配置（有利于制作 Secret）。
// FlattenConfig 把以文件路径引用的证书/密钥读取进内存，生成不依赖外部文件的配置，
// 以便存入 Secret 后在任何机器上都能使用。路径先相对 LocationOfOrigin 解析。
func FlattenConfig(config *Config) error {
	for key, authInfo := range config.AuthInfos {
		baseDir, err := MakeAbs(filepath.Dir(authInfo.LocationOfOrigin), "")
		if err != nil {
			return err
		}

		if err := FlattenContent(&authInfo.ClientCertificate, &authInfo.ClientCertificateData, baseDir); err != nil {
			return err
		}
		if err := FlattenContent(&authInfo.ClientKey, &authInfo.ClientKeyData, baseDir); err != nil {
			return err
		}

		config.AuthInfos[key] = authInfo
	}
	for key, cluster := range config.Clusters {
		baseDir, err := MakeAbs(filepath.Dir(cluster.LocationOfOrigin), "")
		if err != nil {
			return err
		}

		if err := FlattenContent(&cluster.CertificateAuthority, &cluster.CertificateAuthorityData, baseDir); err != nil {
			return err
		}

		config.Clusters[key] = cluster
	}

	return nil
}

// FlattenContent 读取 path 指向的文件写入 contents，并清空 path。
// path 与 contents 同时有值时返回错误，因为语义上二者互斥，无法判断应以哪个为准。
func FlattenContent(path *string, contents *[]byte, baseDir string) error {
	if len(*path) != 0 {
		if len(*contents) > 0 {
			return errors.New("cannot have values for both path and contents")
		}

		var err error
		absPath := ResolvePath(*path, baseDir)
		*contents, err = os.ReadFile(absPath)
		if err != nil {
			return err
		}

		*path = ""
	}

	return nil
}

// ResolvePath 返回相对给定基准目录的绝对路径。
func ResolvePath(path string, base string) string {
	// 不解析空路径
	// 空路径与绝对路径都原样返回，避免改变已有语义。
	if len(path) > 0 {
		// 不解析绝对路径
		if !filepath.IsAbs(path) {
			return filepath.Join(base, path)
		}
	}

	return path
}

// MakeAbs 把可能为相对的路径转换为绝对路径；base 为空时以当前工作目录为基准。
func MakeAbs(path, base string) (string, error) {
	if filepath.IsAbs(path) {
		return path, nil
	}
	if len(base) == 0 {
		cwd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		base = cwd
	}
	return filepath.Join(base, path), nil
}

// RedactSecrets 把所有敏感值替换为 REDACTED。
// RedactSecrets 依据结构体字段的 datapolicy 标签把敏感字段替换为 REDACTED，
// 用于日志或错误信息中安全地输出配置。
func RedactSecrets(config *Config) error {
	return redactSecrets(reflect.ValueOf(config), false)
}

// redactSecrets 递归遍历值；redact 表示当前值是否位于一个标记为敏感的字段之下。
// 之所以向下传递标志，是为了让敏感字段中的字符串、字节切片被整体替换。
func redactSecrets(curr reflect.Value, redact bool) error {
	redactedBytes = []byte("REDACTED")
	if !curr.IsValid() {
		return nil
	}

	actualCurrValue := curr
	if curr.Kind() == reflect.Pointer {
		actualCurrValue = curr.Elem()
	}

	switch actualCurrValue.Kind() {
	case reflect.Map:
		// 映射的值不可直接 Set，因此只递归检查（敏感替换在更深层的字段上完成）。
		for _, v := range actualCurrValue.MapKeys() {
			err := redactSecrets(actualCurrValue.MapIndex(v), false)
			if err != nil {
				return err
			}
		}
		return nil

	case reflect.String:
		// 仅当处于敏感字段路径上且非零值时才替换，保留空字符串语义。
		if redact {
			if !actualCurrValue.IsZero() {
				actualCurrValue.SetString("REDACTED")
			}
		}
		return nil

	case reflect.Slice:
		// []byte 视为原始数据整体替换；其余切片逐元素递归。
		if actualCurrValue.Type() == reflect.TypeOf([]byte{}) && redact {
			if !actualCurrValue.IsNil() {
				actualCurrValue.SetBytes(redactedBytes)
			}
			return nil
		}
		for i := 0; i < actualCurrValue.Len(); i++ {
			err := redactSecrets(actualCurrValue.Index(i), false)
			if err != nil {
				return err
			}
		}
		return nil

	case reflect.Struct:
		for fieldIndex := 0; fieldIndex < actualCurrValue.NumField(); fieldIndex++ {
			currFieldValue := actualCurrValue.Field(fieldIndex)
			currFieldType := actualCurrValue.Type().Field(fieldIndex)
			// 这里读取 datapolicy 标签而非 json/yaml 标签，因为脱敏关注的是安全策略。
			currYamlTag := currFieldType.Tag.Get("datapolicy")
			currFieldTypeYamlName := strings.Split(currYamlTag, ",")[0]
			if currFieldTypeYamlName != "" {
				err := redactSecrets(currFieldValue, true)
				if err != nil {
					return err
				}
			} else {
				err := redactSecrets(currFieldValue, false)
				if err != nil {
					return err
				}
			}
		}
		return nil

	default:
		return nil
	}
}
