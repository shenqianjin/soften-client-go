package log

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"

	"github.com/shenqianjin/soften-client-go/soften/support/meta"
	"github.com/sirupsen/logrus"
)

var defaultTimeFormatter = "2006/01/02 15:04:05.000000"

var callerPrettyFunc = func(frame *runtime.Frame) (function string, file string) {
	funcName := frame.Function
	// either file or function is displayed
	formattedName := frame.File
	if idx := strings.Index(frame.File, "soften-client-go/"); idx > 0 {
		formattedName = frame.File[idx+17:]
	} else if lastSlash := strings.LastIndex(funcName, "/"); lastSlash > 0 {
		fileName := frame.File
		if fLastSlash := strings.LastIndex(fileName, "/"); fLastSlash > 0 {
			if fLastButOneSlash := strings.LastIndex(fileName[0:fLastSlash], "/"); fLastButOneSlash > 0 {
				formattedName = funcName[:lastSlash] + fileName[fLastButOneSlash:]
			}
		}
	}
	return "", fmt.Sprintf("%s:%d", formattedName, frame.Line)
}

type TextFormatter struct {
	*logrus.TextFormatter
}

func WrapTextFormatter(oriFormatter *logrus.TextFormatter) *TextFormatter {
	if oriFormatter.CallerPrettyfier == nil {
		oriFormatter.CallerPrettyfier = callerPrettyFunc
	}
	if oriFormatter.TimestampFormat == "" {
		oriFormatter.TimestampFormat = defaultTimeFormatter
	}
	formatter := &TextFormatter{TextFormatter: oriFormatter}
	return formatter

}

func (f *TextFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	// override caller
	entry.Caller = getCaller()
	//

	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	formattedTime := entry.Time.Format(f.TimestampFormat)
	reqId := ""
	if r, ok := entry.Data[meta.KeyReqId]; ok && r != nil {
		reqId = fmt.Sprintf("%v", r)
	}
	level := strings.ToUpper(entry.Level.String())
	_, fileLine := callerPrettyFunc(entry.Caller)

	labelBytes := make([]string, 0)
	for key, val := range entry.Data {
		labelBytes = append(labelBytes, fmt.Sprintf("%s=%v", key, val))
	}
	msg := entry.Message
	if len(labelBytes) > 0 {
		_, _ = fmt.Fprintf(b, "%s [%v][%s] %s: {%s} --> %s\n", formattedTime, reqId, level, fileLine, strings.Join(labelBytes, ", "), msg)
	} else {
		_, _ = fmt.Fprintf(b, "%s [%v][%s] %s: %s\n", formattedTime, reqId, level, fileLine, msg)
	}
	return b.Bytes(), nil
}

// ------ caller helper ------

var (
	// qualified package name, cached at first use
	callerIgnorePackages = map[string]bool{
		"github.com/sirupsen/logrus":                    true,
		"github.com/apache/pulsar-client-go/pulsar/log": true,
	}
)

const (
	maximumCallerDepth int = 25
	minimumCallerDepth int = 4
)

// getCaller retrieves the name of the first non-logrus calling function
func getCaller() *runtime.Frame {
	// cache this package's fully-qualified name

	// Restrict the lookback frames to avoid runaway lookups
	pcs := make([]uintptr, maximumCallerDepth)
	depth := runtime.Callers(minimumCallerDepth, pcs)
	frames := runtime.CallersFrames(pcs[:depth])

	for f, again := frames.Next(); again; f, again = frames.Next() {
		pkg := getPackageName(f.Function)

		// If the caller isn't part of this package, we're done
		if _, ok := callerIgnorePackages[pkg]; !ok {
			return &f //nolint:scopelint
		}
	}

	// if we got here, we failed to find the caller's context
	return nil
}

// getPackageName reduces a fully qualified function name to the package name
func getPackageName(f string) string {
	for {
		lastPeriod := strings.LastIndex(f, ".")
		lastSlash := strings.LastIndex(f, "/")
		if lastPeriod > lastSlash {
			f = f[:lastPeriod]
		} else {
			break
		}
	}

	return f
}
