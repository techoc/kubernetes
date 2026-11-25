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

package proxy

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
)

// atomsToAttrs 记录哪些标签的哪些属性需要做 URL 替换。
// 只覆盖 HTML 规范中取值为 URL 的属性；遗漏会导致代理后的页面链接仍指向后端地址。
// 来源：http://www.w3.org/TR/REC-html40/index/attributes.html
//
//	http://www.w3.org/html/wg/drafts/html/master/index.html#attributes-1
var atomsToAttrs = map[atom.Atom]sets.String{
	atom.A:          sets.NewString("href"),
	atom.Applet:     sets.NewString("codebase"),
	atom.Area:       sets.NewString("href"),
	atom.Audio:      sets.NewString("src"),
	atom.Base:       sets.NewString("href"),
	atom.Blockquote: sets.NewString("cite"),
	atom.Body:       sets.NewString("background"),
	atom.Button:     sets.NewString("formaction"),
	atom.Command:    sets.NewString("icon"),
	atom.Del:        sets.NewString("cite"),
	atom.Embed:      sets.NewString("src"),
	atom.Form:       sets.NewString("action"),
	atom.Frame:      sets.NewString("longdesc", "src"),
	atom.Head:       sets.NewString("profile"),
	atom.Html:       sets.NewString("manifest"),
	atom.Iframe:     sets.NewString("longdesc", "src"),
	atom.Img:        sets.NewString("longdesc", "src", "usemap"),
	atom.Input:      sets.NewString("src", "usemap", "formaction"),
	atom.Ins:        sets.NewString("cite"),
	atom.Link:       sets.NewString("href"),
	atom.Object:     sets.NewString("classid", "codebase", "data", "usemap"),
	atom.Q:          sets.NewString("cite"),
	atom.Script:     sets.NewString("src"),
	atom.Source:     sets.NewString("src"),
	atom.Video:      sets.NewString("poster", "src"),

	//TODO: 处理 style 元素中隐藏的 CSS URL。
	// style 元素与 style 属性中的 url(...) 尚未处理，属于已知缺口。
}

// Transport 是处理 text/html 内容的 http.RoundTripper：
// 它会把 HTML 中指向后端的 URL 改写为经由代理服务器前缀访问的 URL，
// 使代理后的页面资源仍能通过代理加载。
// Scheme/Host/PathPrepend 描述代理对外暴露的地址与路径前缀。
type Transport struct {
	Scheme      string
	Host        string
	PathPrepend string

	http.RoundTripper
}

// RoundTrip 实现 http.RoundTripper 接口：转发请求、按需改写重定向与 HTML 响应中的链接。
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	// 添加反向代理相关头，使后端能感知原始请求的 URI、Host 与协议。
	// 使用 EscapedPath 保留原始转义，避免路径中的特殊字符被二次编码或解码。
	forwardedURI := path.Join(t.PathPrepend, req.URL.EscapedPath())
	// path.Join 会去掉末尾斜杠，但末尾斜杠对某些后端有意义，需要补回。
	if strings.HasSuffix(req.URL.Path, "/") {
		forwardedURI = forwardedURI + "/"
	}
	req.Header.Set("X-Forwarded-Uri", forwardedURI)
	if len(t.Host) > 0 {
		req.Header.Set("X-Forwarded-Host", t.Host)
	}
	if len(t.Scheme) > 0 {
		req.Header.Set("X-Forwarded-Proto", t.Scheme)
	}

	// 未显式注入 RoundTripper 时退回默认实现，便于零值 Transport 也能工作。
	rt := t.RoundTripper
	if rt == nil {
		rt = http.DefaultTransport
	}
	resp, err := rt.RoundTrip(req)

	// 把网络层错误转换为 503，让客户端知道是后端不可达而非请求本身有问题。
	if err != nil {
		return nil, errors.NewServiceUnavailable(fmt.Sprintf("error trying to reach service: %v", err))
	}

	// 重定向响应只需改写 Location 头，无需处理响应体。
	if redirect := resp.Header.Get("Location"); redirect != "" {
		targetURL, err := url.Parse(redirect)
		if err != nil {
			return nil, errors.NewInternalError(fmt.Errorf("error trying to parse Location header: %v", err))
		}
		resp.Header.Set("Location", t.rewriteURL(targetURL, req.URL, req.Host))
		return resp, nil
	}

	// 只处理 HTML；先去掉 charset 等参数再比较，避免 "text/html; charset=utf-8" 被漏掉。
	cType := resp.Header.Get("Content-Type")
	cType = strings.TrimSpace(strings.SplitN(cType, ";", 2)[0])
	if cType != "text/html" {
		// 非 HTML 内容原样透传。
		return resp, nil
	}

	return t.rewriteResponse(req, resp)
}

// 编译期断言：Transport 实现 net.RoundTripperWrapper，便于上层解包出内层 RoundTripper。
var _ = net.RoundTripperWrapper(&Transport{})

// WrappedRoundTripper 返回被包装的底层 RoundTripper。
func (rt *Transport) WrappedRoundTripper() http.RoundTripper {
	return rt.RoundTripper
}

// rewriteURL 在目标 URL 与 sourceURL（目标 URL 所在页面）同主机、
// 或与 sourceRequestHost 匹配时，将其改写为经由代理访问的形式；
// 其他主机的 URL 保持原样，避免把外部链接也代理到本地。
func (t *Transport) rewriteURL(url *url.URL, sourceURL *url.URL, sourceRequestHost string) string {
	// 示例：
	//      当 API server 处理对某个 service 的代理请求时（如 /api/v1/namespace/foo/service/bar/proxy/），
	//      sourceURL.Host（即 req.URL.Host）是 service 的 endpoint IP 地址；
	//      sourceRequestHost（即 req.Host）是请求中指定的 Host 头，可能与 sourceURL.Host 不同。
	//      例如用户通过本地的 "kubectl proxy" 发起请求
	//      （localhost:8001/api/v1/namespace/foo/service/bar/proxy/），
	//      则 sourceRequestHost 为 "localhost:8001"。
	//
	//      若 service 返回的 URL 带有非空 host，且 url.Host 等于 sourceURL.Host 或
	//      sourceRequestHost，就不应把它视为完全不同的主机；
	//      改写同主机绝对路径 URL 并追加必要前缀是 API server 的职责
	//      （即 /api/v1/namespace/foo/service/bar/proxy/）。
	// 不同主机或相对路径都不改写：相对路径本就相对于代理后的页面，无需加前缀。
	isDifferentHost := url.Host != "" && url.Host != sourceURL.Host && url.Host != sourceRequestHost
	isRelative := !strings.HasPrefix(url.Path, "/")
	if isDifferentHost || isRelative {
		return url.String()
	}

	// 当目标 URL 已包含 sourceRequestHost，且 Transport 未配置 scheme/host 时，
	// 保留原有 scheme 与 host，避免把有效地址清空。
	if !(url.Host == sourceRequestHost && t.Scheme == "" && t.Host == "") {
		url.Scheme = t.Scheme
		url.Host = t.Host
	}

	origPath := url.Path
	// 若路径已包含代理前缀，说明此前已改写，重复改写会产生前缀叠加。
	if strings.HasPrefix(url.Path, t.PathPrepend) {
		return url.String()
	}
	url.Path = path.Join(t.PathPrepend, url.Path)
	if strings.HasSuffix(origPath, "/") {
		// path.Join 会去掉末尾斜杠，这里补回以保持目录语义。
		url.Path += "/"
	}

	return url.String()
}

// rewriteHTML 扫描 HTML 中带 URL 属性的标签，用 urlRewriter 更新其取值，
// 并把更新后的 HTML 写入 writer。
// 它采用流式 tokenizer，避免把整个文档解析成 DOM 带来的内存开销。
func rewriteHTML(reader io.Reader, writer io.Writer, urlRewriter func(*url.URL) string) error {
	// 注意：这里假定内容是 UTF-8。
	tokenizer := html.NewTokenizer(reader)

	var err error
	for err == nil {
		tokenType := tokenizer.Next()
		switch tokenType {
		case html.ErrorToken:
			err = tokenizer.Err()
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()
			if urlAttrs, ok := atomsToAttrs[token.DataAtom]; ok {
				for i, attr := range token.Attr {
					if urlAttrs.Has(attr.Key) {
						url, err := url.Parse(attr.Val)
						if err != nil {
							// URL 无法解析时不改写也不报错：
							// 不能因为看不懂响应体中的某个链接就让整个代理请求失败。
							continue
						}
						token.Attr[i].Val = urlRewriter(url)
					}
				}
			}
			_, err = writer.Write([]byte(token.String()))
		default:
			_, err = writer.Write(tokenizer.Raw())
		}
	}
	if err != io.EOF {
		return err
	}
	return nil
}

// rewriteResponse 改写 HTML 响应：把指向原始主机的绝对链接改为指向代理 Transport。
// 支持 gzip/deflate 压缩响应；未知编码则原样返回，避免破坏内容。
func (t *Transport) rewriteResponse(req *http.Request, resp *http.Response) (*http.Response, error) {
	// 改写后需要替换 resp.Body，因此负责关闭原始 body。
	origBody := resp.Body
	defer origBody.Close()

	newContent := &bytes.Buffer{}
	var reader io.Reader = origBody
	var writer io.Writer = newContent
	// 按 Content-Encoding 解压/再压缩，保持响应编码与头部一致。
	encoding := resp.Header.Get("Content-Encoding")
	switch encoding {
	case "gzip":
		var err error
		reader, err = gzip.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("errorf making gzip reader: %v", err)
		}
		// 边解压边压缩回 gzip；Close 必须在返回前执行以刷出压缩尾部。
		gzw := gzip.NewWriter(writer)
		defer gzw.Close()
		writer = gzw
	case "deflate":
		var err error
		reader = flate.NewReader(reader)
		flw, err := flate.NewWriter(writer, flate.BestCompression)
		if err != nil {
			return nil, fmt.Errorf("errorf making flate writer: %v", err)
		}
		// 先 Flush 再 Close，确保所有压缩数据都已写入底层缓冲。
		defer func() {
			flw.Close()
			flw.Flush()
		}()
		writer = flw
	case "":
		// 未压缩，直接处理。
	default:
		// 不认识的编码：不要尝试解析，否则可能损坏内容或产生错误输出。
		klog.FromContext(req.Context()).Error(nil, "Proxy encountered unknown encoding for text/html, can't understand this so not fixing links", "encoding", encoding)
		return resp, nil
	}

	urlRewriter := func(targetUrl *url.URL) string {
		return t.rewriteURL(targetUrl, req.URL, req.Host)
	}
	// 注意：writer 可能是压缩 writer，其 Close 由上面的 defer 负责。
	err := rewriteHTML(reader, writer, urlRewriter)
	if err != nil {
		klog.FromContext(req.Context()).Error(err, "Failed to rewrite URLs")
		return resp, err
	}

	resp.Body = io.NopCloser(newContent)
	// 内容已被改写，原 Content-Length 不再正确，必须删除并由新的长度替换。
	//TODO: 是否应在此移除所有 hash/signature 头？
	// 内容变化后，摘要/签名类头也会失效，后续应考虑一并移除。
	resp.Header.Del("Content-Length")
	resp.ContentLength = int64(newContent.Len())

	return resp, err
}
