/*
Copyright 2016 The Kubernetes Authors.

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

package i18n

import (
	"archive/zip"
	"bytes"
	"embed"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/chai2010/gettext-go"

	"k8s.io/klog/v2"
)

//go:embed translations
var translations embed.FS

var knownTranslations = map[string][]string{
	"kubectl": {
		"default",
		"en_US",
		"fr_FR",
		"zh_CN",
		"ja_JP",
		"zh_TW",
		"it_IT",
		"de_DE",
		"ko_KR",
		"pt_BR",
	},
	// 仅供单元测试使用。
	"test": {
		"default",
		"en_US",
	},
}

var (
	lazyLoadTranslationsOnce sync.Once
	LoadTranslationsFunc     = func() error {
		return LoadTranslations("kubectl", nil)
	}
	translationsLoaded bool
)

// 设置用于延迟加载翻译的函数。
// 必须在任何包调用 i18n.T() 之前、于 init() 中完成设置。
// 做法是单独建一个包含 init() 的包，并在所有调用 i18n.T() 的包之前
// 导入它。
//
// 用法示例：
//
//	package myi18n
//
//	import "k8s.io/kubectl/pkg/util/i18n"
//
//	func init() {
//		if err := i18n.SetLoadTranslationsFunc(loadCustomTranslations); err != nil {
//			panic(err)
//		}
//	}
//
//	func loadCustomTranslations() error {
//		// 在此加载自定义翻译……
//	}
//
// 然后在主包或根命令包中按如下方式导入自定义包：
//
//	import (
//		// 其他不依赖 i18n 的导入……
//		_ "example.com/myapp/myi18n"
//		// 其他依赖 i18n 的导入……
//	)
func SetLoadTranslationsFunc(f func() error) error {
	if translationsLoaded {
		return errors.New("translations have already been loaded")
	}
	LoadTranslationsFunc = func() error {
		if err := f(); err != nil {
			return err
		}
		translationsLoaded = true
		return nil
	}
	return nil
}

func loadSystemLanguage() string {
	// 区域设置优先级：LC_ALL、LC_MESSAGES、LANG。
	// Similarly to: https://www.gnu.org/software/gettext/manual/html_node/Locale-Environment-Variables.html
	langStr := os.Getenv("LC_ALL")
	if langStr == "" {
		langStr = os.Getenv("LC_MESSAGES")
	}
	if langStr == "" {
		langStr = os.Getenv("LANG")
	}

	if langStr == "" {
		klog.V(3).Infof("Couldn't find the LC_ALL, LC_MESSAGES or LANG environment variables, defaulting to en_US")
		return "default"
	}
	pieces := strings.Split(langStr, ".")
	if len(pieces) != 2 {
		if langStr != "C" {
			klog.V(3).Infof("Unexpected system language (%s), defaulting to en_US", langStr)
		}
		return "default"
	}
	return pieces[0]
}

func findLanguage(root string, getLanguageFn func() string) string {
	langStr := getLanguageFn()

	translations := knownTranslations[root]
	for ix := range translations {
		if translations[ix] == langStr {
			return langStr
		}
	}
	klog.V(3).Infof("Couldn't find translations for %s, using default", langStr)
	return "default"
}

// 加载翻译文件。getLanguageFn 应返回语言字符串
// （如 'en-US'）；为 nil 时使用 loadSystemLanguage，
// 它读取 'LANG' 环境变量。
func LoadTranslations(root string, getLanguageFn func() string) error {
	if getLanguageFn == nil {
		getLanguageFn = loadSystemLanguage
	}

	langStr := findLanguage(root, getLanguageFn)
	translationFiles := []string{
		fmt.Sprintf("%s/%s/LC_MESSAGES/k8s.po", root, langStr),
		fmt.Sprintf("%s/%s/LC_MESSAGES/k8s.mo", root, langStr),
	}

	// TODO: 遍历目录并加载所有文件。
	buf := new(bytes.Buffer)
	w := zip.NewWriter(buf)

	// 务必检查 Close 返回的错误。
	for _, file := range translationFiles {
		filename := "translations/" + file
		f, err := w.Create(file)
		if err != nil {
			return err
		}
		data, err := translations.ReadFile(filename)
		if err != nil {
			return err
		}
		if _, err := f.Write(data); err != nil {
			return nil
		}
	}
	if err := w.Close(); err != nil {
		return err
	}
	gettext.BindLocale(gettext.New("k8s", root+".zip", buf.Bytes()))
	gettext.SetDomain("k8s")
	gettext.SetLanguage(langStr)
	translationsLoaded = true
	return nil
}

func lazyLoadTranslations() {
	lazyLoadTranslationsOnce.Do(func() {
		if translationsLoaded {
			return
		}
		if err := LoadTranslationsFunc(); err != nil {
			klog.Warning("Failed to load translations")
		}
	})
}

// 翻译字符串，按需把参数代入；
// args 非空时，args[0] 视为复数数量，
// 使用复数形式的翻译。
func T(defaultValue string, args ...int) string {
	lazyLoadTranslations()
	if len(args) == 0 {
		return gettext.PGettext("", defaultValue)
	}
	return fmt.Sprintf(gettext.PNGettext("", defaultValue, defaultValue+".plural", args[0]),
		args[0])
}

// 生成包含翻译后错误文本的错误。
// 替换通过上面的 `T` 函数完成，规则
// 相同。
func Errorf(defaultValue string, args ...int) error {
	return errors.New(T(defaultValue, args...))
}
