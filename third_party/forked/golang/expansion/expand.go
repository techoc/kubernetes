package expansion

import (
	"bytes"
	"unicode/utf8"
)

const (
	operator        = '$'
	referenceOpener = '('
	referenceCloser = ')'
)

// syntaxWrap 用展开语法包裹输入字符串。
func syntaxWrap(input string) string {
	return string(operator) + string(referenceOpener) + input + string(referenceCloser)
}

// MappingFuncFor 返回一个供 Expand 使用的映射函数：按展开规范依次查找上下文 map；
// 找不到时返回用展开语法包裹的原字符串，以便保留未解析的引用。
func MappingFuncFor(context ...map[string]string) func(string) string {
	return func(input string) string {
		for _, vars := range context {
			val, ok := vars[input]
			if ok {
				return val
			}
		}

		return syntaxWrap(input)
	}
}

// Expand 按展开规范替换输入字符串中的变量引用，用给定映射函数解析变量值。
func Expand(input string, mapping func(string) string) string {
	var buf bytes.Buffer
	checkpoint := 0
	for cursor := 0; cursor < len(input); cursor++ {
		if input[cursor] == operator && cursor+1 < len(input) {
			// 把自上次检查点以来的原文写入缓冲区
			buf.WriteString(input[checkpoint:cursor])

			// 按语法尝试读出变量名
			read, isVar, advance := tryReadVariableName(input[cursor+1:])

			if isVar {
				// 成功解析出变量名，映射后写入缓冲区
				buf.WriteString(mapping(read))
			} else {
				// 不是变量名，原样写入读到的内容
				buf.WriteString(read)
			}

			// 游标跳过读变量名表达式所消耗的字节
			cursor += advance

			// 更新检查点
			checkpoint = cursor + 1
		}
	}

	// 返回缓冲区内容以及输入字符串中尚未写入的尾部。
	return buf.String() + input[checkpoint:]
}

// tryReadVariableName 尝试从输入中读取变量名，返回读到的内容、该内容是否为需要映射的
// 变量名，以及消耗的字节数。
//
// 调用方保证输入中不含起始操作符。
func tryReadVariableName(input string) (string, bool, int) {
	r, size := utf8.DecodeRuneInString(input)
	switch r {
	case operator:
		// 转义的操作符，原样返回。
		return input[0:size], false, size
	case referenceOpener:
		// 扫描到表达式结束符
		for i := 1; i < len(input); i++ {
			if input[i] == referenceCloser {
				return input[1:i], true, i + 1
			}
		}

		// 引用不完整，原样返回。
		return string(operator) + string(referenceOpener), false, 1
	default:
		// 并非表达式起始（操作符后未跟左括号），返回操作符及随后的第一个字符。

		return string(operator) + string(r), false, size
	}
}
