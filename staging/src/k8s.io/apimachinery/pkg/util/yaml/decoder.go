/*
Copyright 2014 The Kubernetes Authors.

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

package yaml

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"

	jsonutil "k8s.io/apimachinery/pkg/util/json"

	"sigs.k8s.io/yaml"
)

// Unmarshal 反序列化给定数据。
// 当 v 为 *map[string]interface{}、*[]interface{} 或 *interface{} 时，
// 数字会被转换为 int64 或 float64（而不是默认的 float64），以避免大整数丢精度。
func Unmarshal(data []byte, v interface{}) error {
	preserveIntFloat := func(d *json.Decoder) *json.Decoder {
		// UseNumber 让 JSON 解码保留数字字面量，后续再按需转换为 int64/float64。
		d.UseNumber()
		return d
	}
	switch v := v.(type) {
	case *map[string]interface{}:
		if err := yaml.Unmarshal(data, v, preserveIntFloat); err != nil {
			return err
		}
		return jsonutil.ConvertMapNumbers(*v, 0)
	case *[]interface{}:
		if err := yaml.Unmarshal(data, v, preserveIntFloat); err != nil {
			return err
		}
		return jsonutil.ConvertSliceNumbers(*v, 0)
	case *interface{}:
		if err := yaml.Unmarshal(data, v, preserveIntFloat); err != nil {
			return err
		}
		return jsonutil.ConvertInterfaceNumbers(v, 0)
	default:
		return yaml.Unmarshal(data, v)
	}
}

// UnmarshalStrict 严格反序列化给定数据（存在重复字段时报错），
// 数字类型处理规则与 Unmarshal 相同。
func UnmarshalStrict(data []byte, v interface{}) error {
	preserveIntFloat := func(d *json.Decoder) *json.Decoder {
		d.UseNumber()
		return d
	}
	switch v := v.(type) {
	case *map[string]interface{}:
		if err := yaml.UnmarshalStrict(data, v, preserveIntFloat); err != nil {
			return err
		}
		return jsonutil.ConvertMapNumbers(*v, 0)
	case *[]interface{}:
		if err := yaml.UnmarshalStrict(data, v, preserveIntFloat); err != nil {
			return err
		}
		return jsonutil.ConvertSliceNumbers(*v, 0)
	case *interface{}:
		if err := yaml.UnmarshalStrict(data, v, preserveIntFloat); err != nil {
			return err
		}
		return jsonutil.ConvertInterfaceNumbers(v, 0)
	default:
		return yaml.UnmarshalStrict(data, v)
	}
}

// ToJSON 把单个 YAML 文档转换为 JSON 文档，或返回错误。
// 若输入看起来已是 JSON，则直接返回，不经过 YAML 路径，
// 以保证错误信息是 JSON 语义的。
func ToJSON(data []byte) ([]byte, error) {
	if IsJSONBuffer(data) {
		return data, nil
	}
	return yaml.YAMLToJSON(data)
}

// YAMLToJSONDecoder 通过把各个 YAML 文档切分后转换来解码流：
// 先把 YAML 正文转为 JSON，再反序列化为目标对象。
type YAMLToJSONDecoder struct {
	reader      Reader
	inputOffset int
}

// NewYAMLToJSONDecoder 从给定流解码 YAML 文档，按 YAML 规范的文档边界逐块处理：
// 每块先经 yaml.YAMLToJSON 转换为 JSON，再交给 JSON 解码器。
func NewYAMLToJSONDecoder(r io.Reader) *YAMLToJSONDecoder {
	reader := bufio.NewReader(r)
	return &YAMLToJSONDecoder{
		reader: NewYAMLReader(reader),
	}
}

// Decode 从流中读取一个 YAML 文档并解码为 JSON 语义，或返回错误。
// 解码规则与 json.Unmarshal 一致，而不是 yaml.Unmarshal。
func (d *YAMLToJSONDecoder) Decode(into interface{}) error {
	bytes, err := d.reader.Read()
	if err != nil && err != io.EOF { //nolint:errorlint
		return err
	}

	if len(bytes) != 0 {
		err := yaml.Unmarshal(bytes, into)
		if err != nil {
			return YAMLSyntaxError{err}
		}
	}
	// 累计已消费的字节数，供调用方跟踪流位置。
	d.inputOffset += len(bytes)
	return err
}

// InputOffset 返回已解码数据在输入流中的累计偏移量。
func (d *YAMLToJSONDecoder) InputOffset() int {
	return d.inputOffset
}

// YAMLDecoder 按块读取对象；数据不足时返回 ErrShortBuffer。
type YAMLDecoder struct {
	r         io.ReadCloser
	scanner   *bufio.Scanner
	remaining []byte
}

// NewDocumentDecoder 从给定流按 YAML 文档边界切块解码。
// 若整块内容无法一次读入，会返回 io.ErrShortBuffer，帮助调用方调整分帧方式。
func NewDocumentDecoder(r io.ReadCloser) io.ReadCloser {
	scanner := bufio.NewScanner(r)
	// the size of initial allocation for buffer 4k
	// 缓冲区初始大小 4K。
	buf := make([]byte, 4*1024)
	// the maximum size used to buffer a token 5M
	// 单个 token 的最大缓冲上限 5M。
	scanner.Buffer(buf, 5*1024*1024)
	scanner.Split(splitYAMLDocument)
	return &YAMLDecoder{
		r:       r,
		scanner: scanner,
	}
}

// Read 读取上一块的剩余内容，或从流中取下一块。
// TODO: switch to readline approach.
func (d *YAMLDecoder) Read(data []byte) (n int, err error) {
	left := len(d.remaining)
	if left == 0 {
		// return the next chunk from the stream
		// 没有剩余数据时，从流中扫描下一块。
		if !d.scanner.Scan() {
			err := d.scanner.Err()
			if err == nil {
				err = io.EOF
			}
			return 0, err
		}
		out := d.scanner.Bytes()
		d.remaining = out
		left = len(out)
	}

	// fits within data
	// 调用方缓冲足够，直接整块拷出。
	if left <= len(data) {
		copy(data, d.remaining)
		d.remaining = nil
		return left, nil
	}

	// caller will need to reread
	// 缓冲不够，只拷出部分并保留剩余，返回 ErrShortBuffer 提示调用方重新读取。
	copy(data, d.remaining[:len(data)])
	d.remaining = d.remaining[len(data):]
	return len(data), io.ErrShortBuffer
}

// Close 关闭底层读取器。
func (d *YAMLDecoder) Close() error {
	return d.r.Close()
}

const yamlSeparator = "\n---"
const separator = "---"

// splitYAMLDocument 是 bufio.SplitFunc：把 YAML 流按文档分隔符切分为单个文档。
func splitYAMLDocument(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	sep := len([]byte(yamlSeparator))
	if i := bytes.Index(data, []byte(yamlSeparator)); i >= 0 {
		// We have a potential document terminator
		// 找到潜在的分隔位置，需确认其后确实有换行才算完整文档。
		i += sep
		after := data[i:]
		if len(after) == 0 {
			// we can't read any more characters
			// 分隔符后暂无内容：EOF 时取出完整文档，否则等待更多数据。
			if atEOF {
				return len(data), data[:len(data)-sep], nil
			}
			return 0, nil, nil
		}
		if j := bytes.IndexByte(after, '\n'); j >= 0 {
			// 分隔符后找到换行，吞掉该换行并返回当前文档。
			return i + j + 1, data[0 : i-sep], nil
		}
		return 0, nil, nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	// 已到 EOF 时，最后一段没有终止分隔符，直接作为文档返回。
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	// 需要更多数据才能确定边界。
	return 0, nil, nil
}

// YAMLOrJSONDecoder 尝试解码 JSON 或 YAML 文档流。
// JSON 虽然是 YAML 的子集，但 Go 的 JSON 解码把多文档流定义为连续的 JSON 对象
// （如 {}{}），而 YAML 定义为以 "---" 分隔的文档序列。
//
// 该解码器先尝试按 JSON 解码，失败后切换为 YAML。
// 一旦确定是 JSON 流（发现多于一个 JSON 对象），便不再切换到 YAML；
// 一旦切换到 YAML，也不再切回 JSON。
type YAMLOrJSONDecoder struct {
	json         *json.Decoder
	jsonConsumed int64 // of the stream total, how much was JSON?
	yaml         *YAMLToJSONDecoder
	yamlConsumed int64 // of the stream total, how much was YAML?
	stream       *StreamReader
	count        int // how many objects have been decoded
}

// JSONSyntaxError 包装 JSON 语法错误并保留其在流中的偏移。
type JSONSyntaxError struct {
	Offset int64
	Err    error
}

func (e JSONSyntaxError) Error() string {
	return fmt.Sprintf("json: offset %d: %s", e.Offset, e.Err.Error())
}

// YAMLSyntaxError 包装 YAML 语法错误。
type YAMLSyntaxError struct {
	err error
}

func (e YAMLSyntaxError) Error() string {
	return e.err.Error()
}

// NewYAMLOrJSONDecoder 返回能从给定 reader 处理 YAML 或 JSON 文档流的解码器。
// bufferSize 决定解码器向后探查多远来判断是否为 JSON 流
// （判断依据是空白之后是否紧跟左花括号）。
func NewYAMLOrJSONDecoder(r io.Reader, bufferSize int) *YAMLOrJSONDecoder {
	d := &YAMLOrJSONDecoder{}

	reader, _, mightBeJSON := GuessJSONStream(r, bufferSize)
	d.stream = reader
	if mightBeJSON {
		d.json = json.NewDecoder(reader)
	} else {
		d.yaml = NewYAMLToJSONDecoder(reader)
	}
	return d
}

// Decode 从底层流解码下一个对象到 into，或返回错误。
func (d *YAMLOrJSONDecoder) Decode(into interface{}) error {
	// Because we don't know if this is a JSON or YAML stream, a failure from
	// both decoders is ambiguous. When in doubt, it will return the error from
	// the JSON decoder.  Unfortunately, this means that if the first document
	// is invalid YAML, the error won't be awesome.
	// TODO: the errors from YAML are not great, we could improve them a lot.
	// 因为无法预知流的真实格式，两个解码器都失败时错误是歧义的；
	// 此时优先返回 JSON 解码器的错误。
	var firstErr error
	if d.json != nil {
		err := d.json.Decode(into)
		if err == nil {
			d.count++
			// 统计 JSON 已消费的字节数，保持流位置同步。
			consumed := d.json.InputOffset() - d.jsonConsumed
			d.stream.Consume(int(consumed))
			d.jsonConsumed += consumed
			return nil
		}
		if err == io.EOF { //nolint:errorlint
			return err
		}
		var syntax *json.SyntaxError
		if ok := errors.As(err, &syntax); ok {
			firstErr = JSONSyntaxError{
				Offset: syntax.Offset,
				Err:    syntax,
			}
		} else {
			firstErr = err
		}
		if d.count > 1 {
			// If we found 0 or 1 JSON object(s), this stream is still
			// ambiguous.  But if we found more than 1 JSON object, then this
			// is an unambiguous JSON stream, and we should not switch to YAML.
			// 已成功解析多于 1 个 JSON 对象，流格式已确定为 JSON，不再切换。
			return err
		}
		// If JSON decoding hits the end of one object and then fails on the
		// next, it leaves any leading whitespace in the buffer, which can
		// confuse the YAML decoder. We just eat any whitespace we find, up to
		// and including the first newline.
		// JSON 失败时缓冲区可能残留前导空白，先退回再吞掉空白，
		// 避免干扰后续 YAML 解码。
		d.stream.Rewind()
		if err := d.consumeWhitespace(); err == nil {
			d.yaml = NewYAMLToJSONDecoder(d.stream)
		}
		d.json = nil
	}
	if d.yaml != nil {
		err := d.yaml.Decode(into)
		if err == nil {
			d.count++
			// 同理，记录 YAML 已消费的字节数。
			consumed := int64(d.yaml.InputOffset()) - d.yamlConsumed
			d.stream.Consume(int(consumed))
			d.yamlConsumed += consumed
			return nil
		}
		if err == io.EOF { //nolint:errorlint
			return err
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return firstErr
	}
	return fmt.Errorf("decoding failed as both JSON and YAML")
}

// consumeWhitespace 吞掉流开头的空白，直到第一个换行（含）为止。
// 用于在 JSON 切换到 YAML 之前清理缓冲区中的残留空白。
func (d *YAMLOrJSONDecoder) consumeWhitespace() error {
	consumed := 0
	for {
		buf, err := d.stream.ReadN(4)
		if err != nil && err == io.EOF { //nolint:errorlint
			return err
		}
		r, sz := utf8.DecodeRune(buf)
		if r == utf8.RuneError || sz == 0 {
			return fmt.Errorf("invalid utf8 rune")
		}
		// 回退多余读取的字节，只消费当前 rune 所需的字节。
		d.stream.RewindN(len(buf) - sz)
		if !unicode.IsSpace(r) {
			// 遇到非空白：把该 rune 还回去，只消费前面的空白。
			d.stream.RewindN(sz)
			d.stream.Consume(consumed)
			return nil
		}
		consumed += sz
		if r == '\n' {
			// 吞到第一个换行为止，之后交给 YAML 解码器。
			d.stream.Consume(consumed)
			return nil
		}
		if err == io.EOF { //nolint:errorlint
			break
		}
	}
	return io.EOF
}

// Reader 抽象“读取下一个完整文档”的读取器。
type Reader interface {
	Read() ([]byte, error)
}

// YAMLReader 按行读取并在文档分隔符处切分出完整 YAML 文档。
type YAMLReader struct {
	reader Reader
}

// NewYAMLReader 基于 bufio.Reader 创建 YAMLReader。
func NewYAMLReader(r *bufio.Reader) *YAMLReader {
	return &YAMLReader{
		reader: &LineReader{reader: r},
	}
}

// Read 返回一个完整的 YAML 文档。
func (r *YAMLReader) Read() ([]byte, error) {
	var buffer bytes.Buffer
	for {
		line, err := r.reader.Read()
		if err != nil && err != io.EOF { //nolint:errorlint
			return nil, err
		}

		sep := len([]byte(separator))
		if i := bytes.Index(line, []byte(separator)); i == 0 {
			// We have a potential document terminator
			// 行首出现分隔符：其后只允许空白与注释，否则视为非法分隔符。
			i += sep
			trimmed := strings.TrimSpace(string(line[i:]))
			// We only allow comments and spaces following the yaml doc separator, otherwise we'll return an error
			if len(trimmed) > 0 && string(trimmed[0]) != "#" {
				return nil, YAMLSyntaxError{
					err: fmt.Errorf("invalid Yaml document separator: %s", trimmed),
				}
			}
			if buffer.Len() != 0 {
				return buffer.Bytes(), nil
			}
			if err == io.EOF { //nolint:errorlint
				return nil, err
			}
		}
		if err == io.EOF { //nolint:errorlint
			if buffer.Len() != 0 {
				// If we're at EOF, we have a final, non-terminated line. Return it.
				// EOF 时若还有内容，它就是最后一段未以分隔符结尾的文档。
				return buffer.Bytes(), nil
			}
			return nil, err
		}
		buffer.Write(line)
	}
}

// LineReader 按行读取底层 bufio.Reader。
type LineReader struct {
	reader *bufio.Reader
}

// Read 从底层读取一行（包含结尾的 '\n'）。
// 仅当底层读取器出错时才返回错误。
func (r *LineReader) Read() ([]byte, error) {
	var (
		isPrefix bool  = true
		err      error = nil
		line     []byte
		buffer   bytes.Buffer
	)

	// 超长行会被 ReadLine 分段返回，用 isPrefix 判断是否还有后续片段。
	for isPrefix && err == nil {
		line, isPrefix, err = r.reader.ReadLine()
		buffer.Write(line)
	}
	buffer.WriteByte('\n')
	return buffer.Bytes(), err
}

// GuessJSONStream 最多扫描 size 字节，查找指示 JSON 的左花括号，
// 并返回供消费方使用的 bufio.Reader。
func GuessJSONStream(r io.Reader, size int) (*StreamReader, []byte, bool) {
	buffer := NewStreamReader(r, size)
	b, _ := buffer.Peek(size)
	return buffer, b, IsJSONBuffer(b)
}

// IsJSONBuffer 扫描给定缓冲区，查找指示 JSON 的左花括号。
func IsJSONBuffer(buf []byte) bool {
	return hasPrefix(buf, jsonPrefix)
}

var jsonPrefix = []byte("{")

// hasPrefix 判断 buf 中第一个非空白字节是否以 prefix 开头。
func hasPrefix(buf []byte, prefix []byte) bool {
	trim := bytes.TrimLeftFunc(buf, unicode.IsSpace)
	return bytes.HasPrefix(trim, prefix)
}
