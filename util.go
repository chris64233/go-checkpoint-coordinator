package gocheckpointcoordinator

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func intToString(v int64) string {
	return strconv.FormatInt(v, 10)
}
