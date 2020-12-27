package main

import (
	"strings"
)

const (
	keyArg              = "--key="
	keyFileArg          = "--keyfile="
	secretArg           = "secret="
	optionsArgSeparator = ','
	strippedKey         = "--key=***stripped***"
	strippedKeyFile     = "--keyfile=***stripped***"
	strippedSecret      = "secret=***stripped***"
)

// StripSecretInArgs strips values of either "--key"/"--keyfile" or "secret=".
// `args` is left unchanged.
// Expects only one occurrence of either "--key"/"--keyfile" or "secret=".
func StripSecretInArgs(args []string) []string {
	out := make([]string, len(args))
	copy(out, args)

	if !stripKey(out) {
		stripSecret(out)
	}

	return out
}

func stripKey(out []string) bool {
	for i := range out {
		if strings.HasPrefix(out[i], keyArg) {
			out[i] = strippedKey
			return true
		}

		if strings.HasPrefix(out[i], keyFileArg) {
			out[i] = strippedKeyFile
			return true
		}
	}

	return false
}

func stripSecret(out []string) bool {
	for i := range out {
		arg := out[i]
		begin := strings.Index(arg, secretArg)

		if begin == -1 {
			continue
		}

		end := strings.IndexByte(arg[begin+len(secretArg):], optionsArgSeparator)

		out[i] = arg[:begin] + strippedSecret
		if end != -1 {
			out[i] += arg[end+len(secretArg):]
		}

		return true
	}

	return false
}
