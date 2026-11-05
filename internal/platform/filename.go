package platform

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// MaxFilenameBytes is the maximum byte length for a filename. It leaves
// room for a temporary suffix (".<id>.part") within common filesystem
// component limits (255 bytes).
const MaxFilenameBytes = 200

// reservedDeviceNames are Windows reserved device names. The check is
// case-insensitive and also applies to the superscript-digit forms.
var reservedDeviceNames = map[string]struct{}{
	"CON": {}, "PRN": {}, "AUX": {}, "NUL": {},
	"COM1": {}, "COM2": {}, "COM3": {}, "COM4": {}, "COM5": {},
	"COM6": {}, "COM7": {}, "COM8": {}, "COM9": {},
	"LPT1": {}, "LPT2": {}, "LPT3": {}, "LPT4": {}, "LPT5": {},
	"LPT6": {}, "LPT7": {}, "LPT8": {}, "LPT9": {},
	"COM¹": {}, "COM²": {}, "COM³": {},
	"LPT¹": {}, "LPT²": {}, "LPT³": {},
}

// isReservedDeviceName reports whether name's stem (the part before the
// first '.') matches a Windows reserved device name.
func isReservedDeviceName(name string) bool {
	stem := name
	if idx := strings.Index(name, "."); idx >= 0 {
		stem = name[:idx]
	}
	if stem == "" {
		return false
	}
	_, ok := reservedDeviceNames[strings.ToUpper(stem)]
	return ok
}

// ValidateFilename reports whether name is a safe, portable filename.
// It rejects empty names, "." and "..", path separators, control
// characters, Windows-forbidden characters, leading/trailing spaces or
// trailing dots, names longer than MaxFilenameBytes, invalid UTF-8,
// and Windows reserved device names.
func ValidateFilename(name string) error {
	if name == "" {
		return fmt.Errorf("%w: empty filename", domain.ErrInvalidFilename)
	}
	if name == "." || name == ".." {
		return fmt.Errorf("%w: %q is not a valid filename", domain.ErrInvalidFilename, name)
	}
	if !utf8.ValidString(name) {
		return fmt.Errorf("%w: filename is not valid UTF-8", domain.ErrInvalidFilename)
	}
	if len(name) > MaxFilenameBytes {
		return fmt.Errorf("%w: filename exceeds %d bytes", domain.ErrInvalidFilename, MaxFilenameBytes)
	}
	if strings.HasPrefix(name, " ") || strings.HasSuffix(name, " ") {
		return fmt.Errorf("%w: filename must not have leading or trailing spaces", domain.ErrInvalidFilename)
	}
	if strings.HasSuffix(name, ".") {
		return fmt.Errorf("%w: filename must not end with a dot", domain.ErrInvalidFilename)
	}
	for _, r := range name {
		if r == '/' || r == '\\' {
			return fmt.Errorf("%w: filename must not contain path separators", domain.ErrInvalidFilename)
		}
		if r <= 0x1F || r == 0x7F {
			return fmt.Errorf("%w: filename contains control characters", domain.ErrInvalidFilename)
		}
		switch r {
		case '<', '>', ':', '"', '|', '?', '*':
			return fmt.Errorf("%w: filename contains forbidden character %q", domain.ErrInvalidFilename, r)
		}
	}
	if isReservedDeviceName(name) {
		return fmt.Errorf("%w: %q is a reserved device name", domain.ErrInvalidFilename, name)
	}
	return nil
}

// SanitizeFilename converts an untrusted raw filename into one guaranteed
// to pass ValidateFilename. It strips directory components, replaces
// forbidden and control characters with '_', trims spaces and trailing
// dots, prefixes "_" when the stem collides with a Windows reserved
// device name, and truncates to at most MaxFilenameBytes without
// splitting multi-byte UTF-8 sequences.
func SanitizeFilename(raw string) string {
	// Strip directory components on both separators so neither Unix nor
	// Windows traversal sequences survive.
	name := raw
	for _, sep := range []string{"/", "\\"} {
		if idx := strings.LastIndex(name, sep); idx >= 0 {
			name = name[idx+1:]
		}
	}

	// Replace control and Windows-forbidden characters with '_'.
	// Invalid UTF-8 bytes are also replaced via utf8 decoding.
	var b strings.Builder
	b.Grow(len(name))
	for len(name) > 0 {
		r, size := utf8.DecodeRuneInString(name)
		if r == utf8.RuneError && size <= 1 {
			b.WriteRune('_')
			name = name[1:]
			continue
		}
		if r <= 0x1F || r == 0x7F || isForbiddenRune(r) {
			b.WriteRune('_')
		} else {
			b.WriteRune(r)
		}
		name = name[size:]
	}
	name = b.String()

	name = strings.Trim(name, " ")
	name = strings.TrimRight(name, ".")
	name = strings.Trim(name, " ")

	if name == "" || name == "." || name == ".." {
		return "download"
	}

	// Prefix reserved device names to make them ordinary filenames.
	if isReservedDeviceName(name) {
		name = "_" + name
	}

	// Truncate to MaxFilenameBytes without splitting runes, preserving
	// the extension when it fits reasonably.
	if len(name) > MaxFilenameBytes {
		ext := fileExtension(name)
		if ext != "" && len(ext) < MaxFilenameBytes {
			stem := strings.TrimSuffix(name, ext)
			// Reserve space for the extension; if the stem is empty
			// after trimming, fall back to truncating the whole name.
			if stem != "" {
				limit := MaxFilenameBytes - len(ext)
				name = truncateUTF8(stem, limit) + ext
			} else {
				name = truncateUTF8(name, MaxFilenameBytes)
			}
		} else {
			name = truncateUTF8(name, MaxFilenameBytes)
		}
	}

	// Truncation may have exposed trailing spaces/dots again.
	name = strings.Trim(name, " ")
	name = strings.TrimRight(name, ".")

	if name == "" || name == "." || name == ".." {
		return "download"
	}
	// Re-check reserved names after truncation.
	if isReservedDeviceName(name) {
		stem := name
		ext := fileExtension(name)
		if ext != "" {
			stem = strings.TrimSuffix(name, ext)
		}
		_ = stem
		name = "_" + name
		if len(name) > MaxFilenameBytes {
			name = truncateUTF8(name, MaxFilenameBytes)
		}
	}

	// Final guarantee: if it still fails validation, fall back.
	if err := ValidateFilename(name); err != nil {
		return "download"
	}
	return name
}

func isForbiddenRune(r rune) bool {
	switch r {
	case '<', '>', ':', '"', '|', '?', '*', '/', '\\':
		return true
	}
	return false
}

// fileExtension returns the final extension including the dot, e.g.
// ".gz" for "archive.tar.gz". It returns "" for names without a
// meaningful extension or that begin with a dot (".hidden").
func fileExtension(name string) string {
	idx := strings.LastIndex(name, ".")
	if idx <= 0 {
		return ""
	}
	return name[idx:]
}

// truncateUTF8 returns s truncated to at most maxBytes bytes without
// splitting a multi-byte rune. It also avoids leaving trailing spaces
// or dots created by the cut.
func truncateUTF8(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	// Walk runes accumulating byte width until the budget is exceeded.
	count := 0
	for i, r := range s {
		w := utf8.RuneLen(r)
		if w < 0 {
			w = 1
		}
		if count+w > maxBytes {
			s = s[:i]
			break
		}
		count += w
	}
	s = strings.Trim(s, " ")
	s = strings.TrimRight(s, ".")
	return s
}

// splitStemExt splits filename into stem and extension. For names like
// "archive.tar.gz" the extension is ".gz" and the stem is
// "archive.tar". For names without an extension, ext is "".
func splitStemExt(filename string) (stem, ext string) {
	ext = fileExtension(filename)
	if ext == "" {
		return filename, ""
	}
	return strings.TrimSuffix(filename, ext), ext
}

// truncateStemTo fits stem+ext within MaxFilenameBytes.
func truncateStemTo(stem, ext string, maxBytes int) string {
	limit := maxBytes - len(ext)
	if limit <= 0 {
		return strings.TrimSuffix(truncateUTF8(stem+ext, maxBytes), ext)
	}
	if len(stem) <= limit {
		return stem
	}
	return truncateUTF8(stem, limit)
}

// NewID generates a random 16-byte identifier encoded as 32 hex chars.
func NewID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("platform: generate id: %w", err)
	}
	return hex.EncodeToString(buf[:]), nil
}

// isSpaceRune is kept for potential future use.
func isSpaceRune(r rune) bool { return unicode.IsSpace(r) }
