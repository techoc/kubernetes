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

package runtime

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/doc"
	"go/parser"
	"go/token"
	"io"
	"reflect"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/errors"
)

// Pair 保存字段名（或类型名）及其对应的文档文本。
type Pair struct {
	Name, Doc string
}

// KubeTypes 按源码顺序保存解析文件中的类型及字段文档；每个元素的第 0 项表示结构体自身。
type KubeTypes []Pair

// astFrom 解析单个 Go 源文件并构造 doc.Package；解析失败时打印错误并返回 nil，
// 调用方需容忍 nil（生成器随后会 panic 或产出空结果）。
func astFrom(filePath string) *doc.Package {
	fset := token.NewFileSet()
	m := make(map[string]*ast.File)

	f, err := parser.ParseFile(fset, filePath, nil, parser.ParseComments)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	m[filePath] = f
	apkg, _ := ast.NewPackage(fset, m, nil, nil)

	return doc.New(apkg, "", 0)
}

// fmtRawDoc 把 godoc 文本压平成可嵌入 Go 字符串字面量的单行文本：
// 丢弃 "---" 之后的内容、单行 TODO 与生成器指令，保留段落与代码块缩进，
// 最后转义引号/换行/制表符。
func fmtRawDoc(rawDoc string) string {
	var buffer bytes.Buffer
	delPrevChar := func() {
		if buffer.Len() > 0 {
			buffer.Truncate(buffer.Len() - 1) // 删除缓冲区末尾的一个空格或换行符。
		}
	}

	// 忽略分隔符 --- 之后的所有内容。
	// "---" 之后通常是示例或补充说明，不属于 swagger 描述。
	rawDoc = strings.Split(rawDoc, "---")[0]

	for _, line := range strings.Split(rawDoc, "\n") {
		line = strings.TrimRight(line, " ")
		leading := strings.TrimLeft(line, " ")
		switch {
		case len(line) == 0: // 保留段落之间的空行
			delPrevChar()
			buffer.WriteString("\n\n")
		case strings.HasPrefix(leading, "TODO"): // 忽略单行 TODO
		case strings.HasPrefix(leading, "+"): // 忽略面向代码生成器的指令
		default:
			// 缩进行视为代码块，前后补换行以保留排版。
			if strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t") {
				delPrevChar()
				line = "\n" + line + "\n" // Replace it with newline. This is useful when we have a line with: "Example:\n\tJSON-someting..."
			} else {
				line += " "
			}
			buffer.WriteString(line)
		}
	}

	postDoc := strings.TrimRight(buffer.String(), "\n")
	postDoc = strings.Replace(postDoc, "\\\"", "\"", -1) // replace user's \" to "
	postDoc = strings.Replace(postDoc, "\"", "\\\"", -1) // Escape "
	postDoc = strings.Replace(postDoc, "\n", "\\n", -1)
	postDoc = strings.Replace(postDoc, "\t", "\\t", -1)

	return postDoc
}

// fieldName 返回字段在 JSON 对象中使用的键名。返回 "-" 表示该字段不参与 JSON 表示。
//
// 与 encoding/json 的命名规则保持一致：显式 tag 优先；无 tag 时用字段名；
// 匿名内嵌字段声明了 json tag 且 tag 名为空时返回 "-"（该字段整体不展开）。
func fieldName(field *ast.Field) (string, error) {
	jsonTag := ""
	if field.Tag != nil {
		var jsonTagExists bool
		tagValue, err := strconv.Unquote(field.Tag.Value)
		if err != nil {
			return "", err
		}
		jsonTag, jsonTagExists = reflect.StructTag(tagValue).Lookup("json") // 去除结构体标签字面量外围的引号后再解析。
		// 匿名嵌入字段显式声明了空 JSON 名称时，整个字段不展开。
		if field.Names == nil && jsonTagExists && (jsonTag == "" || strings.HasPrefix(jsonTag, ",")) {
			return "-", nil
		}
	}

	jsonTag = strings.Split(jsonTag, ",")[0] // This can return "-"
	if jsonTag == "" {
		if field.Names != nil {
			return field.Names[0].Name, nil
		}
		// 匿名字段无 tag：用类型名作为键。
		return field.Type.(*ast.Ident).Name, nil
	}
	return jsonTag, nil
}

// bufferedLine 表示一行待写入文本及其缩进层级。
//
// bufferedLine 保存一行文本及其缩进层级，统一在 flush 时展开为制表符。
type bufferedLine struct {
	line        string
	indentation int
}

// buffer 按顺序收集待输出行，最后一次性写入 io.Writer。
type buffer struct {
	lines []bufferedLine
}

func newBuffer() *buffer {
	return &buffer{
		lines: make([]bufferedLine, 0),
	}
}

func (b *buffer) addLine(line string, indent int) {
	b.lines = append(b.lines, bufferedLine{line, indent})
}

// flushLines 把缓冲的行按缩进展开后写入 w。
func (b *buffer) flushLines(w io.Writer) error {
	for _, line := range b.lines {
		indentation := strings.Repeat("\t", line.indentation)
		fullLine := fmt.Sprintf("%s%s", indentation, line.line)
		if _, err := io.WriteString(w, fullLine); err != nil {
			return err
		}
	}
	return nil
}

// writeFuncHeader 开始一个 map 变量声明，后续 writeMapBody 填充条目。
func writeFuncHeader(b *buffer, structName string, indent int) {
	s := fmt.Sprintf("var map_%s = map[string]string {\n", structName)
	b.addLine(s, indent)
}

// writeFuncFooter 结束 map 声明并生成 SwaggerDoc() 方法。
func writeFuncFooter(b *buffer, structName string, indent int) {
	b.addLine("}\n", indent) // Closes the map definition

	s := fmt.Sprintf("func (%s) SwaggerDoc() map[string]string {\n", structName)
	b.addLine(s, indent)
	s = fmt.Sprintf("return map_%s\n", structName)
	b.addLine(s, indent+1)
	b.addLine("}\n", indent) // Closes the function definition
}

// writeMapBody 为每个字段生成一行 `"name": "doc",` 条目。
func writeMapBody(b *buffer, kubeType []Pair, indent int) {
	format := "\"%s\": \"%s\",\n"
	for _, pair := range kubeType {
		s := fmt.Sprintf(format, pair.Name, pair.Doc)
		b.addLine(s, indent+2)
	}
}

// ParseDocumentationFrom 提取源文件中所有结构体及字段的文档，并按源码顺序返回。
// 每个结构体对应一个数组，首项为结构体自身，后续项为字段；使用数组是为了保证字段顺序。
// 即使结构体或字段没有文档，也会保留为 {name, ""}，以便后续检查缺失项。
//
// 用数组而非 map 保存字段，确保生成顺序与源码一致；无文档的字段仍保留，
// 由 VerifySwaggerDocsExist 负责报告缺失。
func ParseDocumentationFrom(src string) ([]KubeTypes, error) {
	var docForTypes []KubeTypes
	var errs []error

	pkg := astFrom(src)

	for _, kubType := range pkg.Types {
		if structType, ok := kubType.Decl.Specs[0].(*ast.TypeSpec).Type.(*ast.StructType); ok {
			var ks KubeTypes
			ks = append(ks, Pair{kubType.Name, fmtRawDoc(kubType.Doc)})

			for _, field := range structType.Fields.List {
				if n, err := fieldName(field); err != nil {
					errs = append(errs, err)
				} else if n != "-" {
					fieldDoc := fmtRawDoc(field.Doc.Text())
					ks = append(ks, Pair{n, fieldDoc})
				}
			}
			docForTypes = append(docForTypes, ks)
		}
	}

	return docForTypes, errors.NewAggregate(errs)
}

// WriteSwaggerDocFunc 将结构体及字段的文档写成 Go 源码形式的 SwaggerDoc 方法声明，
// 供 Swagger 文档生成流程读取。
func WriteSwaggerDocFunc(kubeTypes []KubeTypes, w io.Writer) error {
	for _, kubeType := range kubeTypes {
		structName := kubeType[0].Name
		kubeType[0].Name = ""

		// 忽略没有文档内容的结构体或字段。
		// 只保留有文档的字段，避免生成大量空字符串条目。
		docfulTypes := make(KubeTypes, 0, len(kubeType))
		for _, pair := range kubeType {
			if pair.Doc != "" {
				docfulTypes = append(docfulTypes, pair)
			}
		}

		if len(docfulTypes) == 0 {
			continue // If no fields and the struct have documentation, skip the function definition
		}

		indent := 0
		buffer := newBuffer()

		writeFuncHeader(buffer, structName, indent)
		writeMapBody(buffer, docfulTypes, indent)
		writeFuncFooter(buffer, structName, indent)
		buffer.addLine("\n", 0)

		if err := buffer.flushLines(w); err != nil {
			return err
		}
	}

	return nil
}

// VerifySwaggerDocsExist 将缺少文档注释的结构体和字段写入 w，并返回缺失项数量。
//
// 返回缺失文档的数量；调用方据此决定是否让 verify 失败。
func VerifySwaggerDocsExist(kubeTypes []KubeTypes, w io.Writer) (int, error) {
	missingDocs := 0
	buffer := newBuffer()

	for _, kubeType := range kubeTypes {
		structName := kubeType[0].Name
		if kubeType[0].Doc == "" {
			format := "Missing documentation for the struct itself: %s\n"
			s := fmt.Sprintf(format, structName)
			buffer.addLine(s, 0)
			missingDocs++
		}
		kubeType = kubeType[1:] // 跳过数组首项（结构体自身），下面只检查字段。

		for _, pair := range kubeType { // 逐个检查结构体字段
			if pair.Doc == "" {
				format := "In struct: %s, field documentation is missing: %s\n"
				s := fmt.Sprintf(format, structName, pair.Name)
				buffer.addLine(s, 0)
				missingDocs++
			}
		}
	}

	if err := buffer.flushLines(w); err != nil {
		return -1, err
	}
	return missingDocs, nil
}
