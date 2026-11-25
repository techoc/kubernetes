/*
Copyright 2017 The Kubernetes Authors.

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

package util

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/util/validation"
)

var utf8bom = []byte{0xEF, 0xBB, 0xBF}

// processEnvFileLine 在该行为空行或注释时返回空的 key。
// 必要时会从环境变量中获取对应的 value。
func processEnvFileLine(line []byte, filePath string,
	currentLine int) (key, value string, err error) {

	if !utf8.Valid(line) {
		return ``, ``, fmt.Errorf("env file %s contains invalid utf8 bytes at line %d: %v",
			filePath, currentLine+1, line)
	}

	// 只去掉文件首行的 UTF8 BOM，其他行不做处理
	if currentLine == 0 {
		line = bytes.TrimPrefix(line, utf8bom)
	}

	// 先去掉行首的所有空白字符
	line = bytes.TrimLeftFunc(line, unicode.IsSpace)

	// 如果该行为空或为注释，则返回空的 key/value 对。
	if len(line) == 0 || line[0] == '#' {
		return ``, ``, nil
	}

	data := strings.SplitN(string(line), "=", 2)
	key = data[0]
	if errs := validation.IsEnvVarName(key); len(errs) != 0 {
		return ``, ``, fmt.Errorf("%q is not a valid key name: %s", key, strings.Join(errs, ";"))
	}

	if len(data) == 2 {
		value = data[1]
	} else {
		// 没有 value（行中没有 `=`）表示需要
		// 从环境变量中获取该值。
		value = os.Getenv(key)
	}
	return
}

// AddFromEnvFile 处理 env 文件，允许通用的 addTo 函数
// 处理收集到的键值对，或在出错时返回错误。
func AddFromEnvFile(filePath string, addTo func(key, value string) error) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	currentLine := 0
	for scanner.Scan() {
		// 处理当前行，尽可能获取一个
		// 键值对。
		scannedBytes := scanner.Bytes()
		key, value, err := processEnvFileLine(scannedBytes, filePath, currentLine)
		if err != nil {
			return err
		}
		currentLine++

		if len(key) == 0 {
			// 没有 key 表示该行为空行或注释
			continue
		}

		if err = addTo(key, value); err != nil {
			return err
		}
	}
	return nil
}
