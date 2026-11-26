/*
Copyright The Kubernetes Authors.

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

// Package manifest 提供从静态清单文件加载准入配置的公共工具。
package manifest

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

// splitYAMLDocuments 将多文档 YAML 字节流拆成单个文档。
// 空文档会被跳过。
//
// 按 `---` 分隔符流式读取而非一次性解析，避免大文件全部载入内存；
// 跳过空文档是因为尾部 `---` 和纯注释文件在 YAML 中合法但没有语义。
func splitYAMLDocuments(data []byte) ([][]byte, error) {
	reader := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
	var docs [][]byte
	for {
		doc, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		doc = bytes.TrimSpace(doc)
		if len(doc) == 0 {
			continue
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

// FileDoc 保存一个解码后的 YAML 文档及其来源文件。
//
// 保留来源路径是为了在解码/校验失败时给出可定位的错误信息；
// 一个文件可能产生多个 FileDoc。
type FileDoc struct {
	FilePath string
	Doc      []byte
}

// LoadFiles 读取 dir 下所有 YAML/JSON 文件，拆分多文档 YAML，
// 返回各文档及其来源路径，并附带基于文件内容、以 sha256: 为前缀的哈希用于变更检测。
// 按文件名字母序处理，保证结果稳定。
//
// 返回的哈希覆盖文件名顺序与内容，任何增删改都会改变哈希；
// 但哈希不含路径本身，目录整体搬迁不会触发无谓的重新加载。
func LoadFiles(dir string) ([]FileDoc, string, error) {
	if len(dir) == 0 {
		return nil, "", fmt.Errorf("manifest directory path is empty")
	}

	// os.ReadDir 按文件名排序返回条目，保证处理顺序稳定。
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read manifest directory %q: %w", dir, err)
	}

	var fileDocs []FileDoc
	h := sha256.New()
	hasData := false

	for _, entry := range entries {
		// 不递归子目录：静态清单目录应保持扁平，避免误加载备份或临时文件。
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		ext := strings.ToLower(filepath.Ext(name))
		// 大小写不敏感地匹配扩展名，兼容 .YAML 等写法。
		if ext != ".yaml" && ext != ".yml" && ext != ".json" {
			continue
		}

		filePath := filepath.Join(dir, name)
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, "", fmt.Errorf("failed to read file %q: %w", filePath, err)
		}
		// 空文件跳过：既不计入哈希也不产生文档。
		if len(data) == 0 {
			continue
		}

		// 写入 0 字节作为文件间分隔符，防止不同文件内容拼接后哈希碰撞。
		h.Write(data)
		h.Write([]byte{0})
		hasData = true

		docs, err := splitYAMLDocuments(data)
		if err != nil {
			return nil, "", fmt.Errorf("failed to split YAML documents in file %q: %w", filePath, err)
		}
		for _, doc := range docs {
			fileDocs = append(fileDocs, FileDoc{FilePath: filePath, Doc: doc})
		}
	}

	// 目录为空或全是空文件时返回空哈希，调用方据此判断"无配置"而非"配置变化"。
	var hash string
	if hasData {
		hash = fmt.Sprintf("sha256:%x", h.Sum(nil))
	}

	return fileDocs, hash, nil
}
