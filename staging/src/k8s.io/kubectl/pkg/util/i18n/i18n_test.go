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
	"os"
	"sync"
	"testing"

	"github.com/chai2010/gettext-go"
)

var knownTestLocale = "en_US.UTF-8"

func TestTranslation(t *testing.T) {
	err := LoadTranslations("test", func() string { return "default" })
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	result := T("test_string")
	if result != "foo" {
		t.Errorf("expected: %s, saw: %s", "foo", result)
	}
}

func TestTranslationPlural(t *testing.T) {
	err := LoadTranslations("test", func() string { return "default" })
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	result := T("test_plural", 3)
	if result != "there were 3 items" {
		t.Errorf("expected: %s, saw: %s", "there were 3 items", result)
	}

	result = T("test_plural", 1)
	if result != "there was 1 item" {
		t.Errorf("expected: %s, saw: %s", "there was 1 item", result)
	}
}

func TestTranslationUsingEnvVar(t *testing.T) {
	// 测试中修改环境变量前必须备份并恢复，
	// 否则可能破坏依赖相同环境变量的其他测试。
	envVarsToBackup := []string{"LC_MESSAGES", "LANG", "LC_ALL"}
	expectedStrEnUSLocale := "baz"
	expectedStrFallback := "foo"

	testCases := []struct {
		name        string
		setenv      map[string]string
		expectedStr string
	}{
		{
			name:        "Only LC_ALL is set",
			setenv:      map[string]string{"LC_ALL": knownTestLocale},
			expectedStr: expectedStrEnUSLocale,
		},
		{
			name:        "Only LC_MESSAGES is set",
			setenv:      map[string]string{"LC_MESSAGES": knownTestLocale},
			expectedStr: expectedStrEnUSLocale,
		},
		{
			name:        "Only LANG",
			setenv:      map[string]string{"LANG": knownTestLocale},
			expectedStr: expectedStrEnUSLocale,
		},
		{
			name: "LC_MESSAGES overrides LANG",
			setenv: map[string]string{
				"LANG":        "be_BY.UTF-8", // Unknown locale
				"LC_MESSAGES": knownTestLocale,
			},
			expectedStr: expectedStrEnUSLocale,
		},
		{
			name: "LC_ALL overrides LANG",
			setenv: map[string]string{
				"LANG":   "be_BY.UTF-8", // Unknown locale
				"LC_ALL": knownTestLocale,
			},
			expectedStr: expectedStrEnUSLocale,
		},
		{
			name: "LC_ALL overrides LC_MESSAGES",
			setenv: map[string]string{
				"LC_MESSAGES": "be_BY.UTF-8", // Unknown locale
				"LC_ALL":      knownTestLocale,
			},
			expectedStr: expectedStrEnUSLocale,
		},
		{
			name:        "Unknown locale in LANG",
			setenv:      map[string]string{"LANG": "be_BY.UTF-8"},
			expectedStr: expectedStrFallback,
		},
		{
			name:        "Unknown locale in LC_MESSAGES",
			setenv:      map[string]string{"LC_MESSAGES": "be_BY.UTF-8"},
			expectedStr: expectedStrFallback,
		},
		{
			name:        "Unknown locale in LC_ALL",
			setenv:      map[string]string{"LC_ALL": "be_BY.UTF-8"},
			expectedStr: expectedStrFallback,
		},
		{
			name:        "Invalid env var",
			setenv:      map[string]string{"LC_MESSAGES": "fake.locale.UTF-8"},
			expectedStr: expectedStrFallback,
		},
		{
			name:        "No env vars",
			expectedStr: expectedStrFallback,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			for _, envVar := range envVarsToBackup {
				if envVarValue := os.Getenv(envVar); envVarValue != "" {
					envVarValue, envVar := envVarValue, envVar
					t.Cleanup(func() { os.Setenv(envVar, envVarValue) })
					os.Unsetenv(envVar)
				}
			}

			for envVar, envVarValue := range test.setenv {
				t.Setenv(envVar, envVarValue)
			}

			err := LoadTranslations("test", nil)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			result := T("test_string")
			if result != test.expectedStr {
				t.Errorf("expected: %s, saw: %s", test.expectedStr, result)
			}
		})
	}
}

// 重置状态，让多个测试都能验证翻译的延迟加载。
func resetLazyLoading() {
	translationsLoaded = false
	lazyLoadTranslationsOnce = sync.Once{}
}

func TestLazyLoadTranslationFuncIsCalled(t *testing.T) {
	resetLazyLoading()

	timesCalled := 0
	err := SetLoadTranslationsFunc(func() error {
		timesCalled++
		return LoadTranslations("test", func() string { return "en_US" })
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if translationsLoaded {
		t.Errorf("expected translationsLoaded to be false, but it was true")
	}

	// 翻译应成功，并使用延迟加载的翻译。
	result := T("test_string")
	if result != "baz" {
		t.Errorf("expected: %s, saw: %s", "baz", result)
	}
	if timesCalled != 1 {
		t.Errorf("expected LoadTranslationsFunc to have been called 1 time, but it was called %d times", timesCalled)
	}
	if !translationsLoaded {
		t.Errorf("expected translationsLoaded to be true, but it was false")
	}

	// 再次调用 T()，timesCalled 应仍为 1。
	T("test_string")
	if timesCalled != 1 {
		t.Errorf("expected LoadTranslationsFunc to have been called 1 time, but it was called %d times", timesCalled)
	}
}

func TestLazyLoadTranslationFuncOnlyCalledIfTranslationsNotLoaded(t *testing.T) {
	resetLazyLoading()

	// 设置自定义翻译函数。
	timesCalled := 0
	err := SetLoadTranslationsFunc(func() error {
		timesCalled++
		return LoadTranslations("test", func() string { return "en_US" })
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if translationsLoaded {
		t.Errorf("expected translationsLoaded to be false, but it was true")
	}

	// 在延迟加载发生前显式加载翻译。
	err = LoadTranslations("test", func() string { return "default" })
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !translationsLoaded {
		t.Errorf("expected translationsLoaded to be true, but it was false")
	}

	// 翻译应使用显式加载的版本，而非延迟加载的。
	result := T("test_string")
	if result != "foo" {
		t.Errorf("expected: %s, saw: %s", "foo", result)
	}
	if timesCalled != 0 {
		t.Errorf("expected LoadTranslationsFunc to have not been called, but it was called %d times", timesCalled)
	}
}

func TestSetCustomLoadTranslationsFunc(t *testing.T) {
	resetLazyLoading()

	// 设置从目录加载翻译的自定义函数。
	err := SetLoadTranslationsFunc(func() error {
		gettext.BindLocale(gettext.New("k8s", "./translations/test"))
		gettext.SetDomain("k8s")
		gettext.SetLanguage("en_US")
		return nil
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if translationsLoaded {
		t.Errorf("expected translationsLoaded to be false, but it was true")
	}

	// 翻译应成功。
	result := T("test_string")
	if result != "baz" {
		t.Errorf("expected: %s, saw: %s", "baz", result)
	}
	if !translationsLoaded {
		t.Errorf("expected translationsLoaded to be true, but it was false")
	}
}

func TestSetCustomLoadTranslationsFuncAfterTranslationsLoadedShouldFail(t *testing.T) {
	resetLazyLoading()

	// 显式加载翻译。
	err := LoadTranslations("test", func() string { return "en_US" })
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !translationsLoaded {
		t.Errorf("expected translationsLoaded to be true, but it was false")
	}

	// 翻译已加载，此时应失败，且不应调用自定义函数。
	timesCalled := 0
	err = SetLoadTranslationsFunc(func() error {
		timesCalled++
		return nil
	})
	if err == nil {
		t.Errorf("expected error, but it did not occur")
	}
	if timesCalled != 0 {
		t.Errorf("expected LoadTranslationsFunc to have not been called, but it was called %d times", timesCalled)
	}
}
