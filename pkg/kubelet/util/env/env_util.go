/*
Copyright 2025 The Kubernetes Authors.

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
	"bufio"
	"fmt"
	"os"
	"strings"
)

// ParseEnv 使用 POSIX shell 语法的一个子集严格解析环境变量文件。
// 解析器要求所有变量值都必须用单引号包裹。
//
// 支持的格式：
//   - VAR='value' - 值必须用单引号包裹
//   - 单引号内的内容按字面保留（不处理转义或展开）
//   - 支持多行值（保留单引号内的换行）
//   - 支持闭合引号后的行内注释（如 VAR='value' # comment）
//   - 忽略变量名前的空白
//   - 忽略空行（包括仅含空白的行），引号内除外
//   - 以 '#' 开头的行视为注释并忽略
//   - '=' 前不允许空白（如 VAR = 'value' 会被拒绝）
//   - '=' 后、引号前的空白会导致空赋值（如 VAR= 'value' 赋值为空字符串）
func ParseEnv(envFilePath, key string) (string, error) {
	file, err := os.Open(envFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to open environment variable file %q: %w", envFilePath, err)
	}
	defer func() { _ = file.Close() }()

	// 逐行扫描；单引号内的换行由内层循环继续读取。
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		// 忽略变量名前的缩进。
		line = strings.TrimLeft(line, " \t")

		// 跳过空行。
		if line == "" {
			continue
		}

		// 跳过整行注释。
		if strings.HasPrefix(line, "#") {
			continue
		}

		// 缺少等号说明格式非法。
		eqIdx := strings.Index(line, "=")
		if eqIdx == -1 {
			return "", fmt.Errorf("invalid environment variable format at line %d: missing '='", lineNum)
		}

		// 变量名不能包含空白，且 '=' 前不能有尾随空白。
		varNamePart := line[:eqIdx]
		varName := strings.TrimRight(varNamePart, " \t")
		if varName == "" {
			return "", fmt.Errorf("invalid environment variable format at line %d: empty variable name", lineNum)
		}

		// 若右裁剪改变了内容，说明 '=' 前存在空白，应拒绝。
		if varNamePart != varName {
			return "", fmt.Errorf("invalid environment variable format at line %d: whitespace before '=' is not allowed", lineNum)
		}
		valuePart := line[eqIdx+1:]

		// 检查等号后是否存在空白前缀。
		trimmedValue := strings.TrimLeft(valuePart, " \t")
		if valuePart != trimmedValue {
			// 等号与值之间存在空白，与 bash 行为一致：
			// KEY= 'val1' 会使 KEY 的值为空。
			if varName == key {
				return "", nil
			}
			continue
		}

		// 值必须以单引号开头。
		if !strings.HasPrefix(trimmedValue, "'") {
			return "", fmt.Errorf("invalid environment variable format at line %d: value must be enclosed in single quotes", lineNum)
		}

		// 查找闭合单引号（值可能跨多行）。
		var valueBuilder strings.Builder
		rest := trimmedValue[1:]
		startLineNum := lineNum

		// 单引号可能跨行，循环读取直到找到闭合引号。
		for {
			closingIdx := strings.Index(rest, "'")
			if closingIdx != -1 {
				valueBuilder.WriteString(rest[:closingIdx])
				afterQuote := strings.TrimLeft(rest[closingIdx+1:], " \t")
				// 闭合引号后只允许空白或行内注释。
				if afterQuote != "" && !strings.HasPrefix(afterQuote, "#") {
					return "", fmt.Errorf("invalid environment variable format at line %d: unexpected content after closing quote", lineNum)
				}

				if varName == key {
					return valueBuilder.String(), nil
				}
				break
			}

			valueBuilder.WriteString(rest)
			valueBuilder.WriteString("\n")
			// 文件结束仍未闭合，说明格式错误。
			if !scanner.Scan() {
				return "", fmt.Errorf("invalid environment variable format starting at line %d: unclosed single quote", startLineNum)
			}
			lineNum++
			rest = scanner.Text()
		}
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("error reading environment variable file %q: %w", envFilePath, err)
	}

	return "", nil
}
