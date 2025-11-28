package leak_detection

import (
	"runtime"
	"strconv"
	"strings"
)

// GetCurrentGoroutineID returns the current goroutine ID
// Note: This is for debugging purposes only and should not be used in production code
func GetCurrentGoroutineID() uint64 {
	buf := make([]byte, 32)
	n := runtime.Stack(buf, false)
	
	// Parse the stack trace to extract the goroutine ID
	// Format is: "goroutine X [running]:"
	stackLine := string(buf[:n])
	lines := strings.Split(stackLine, "\n")
	if len(lines) > 0 {
		line := lines[0]
		// Find the goroutine ID in the format "goroutine X ["
		if strings.HasPrefix(line, "goroutine ") {
			parts := strings.Split(line, " ")
			if len(parts) > 1 {
				if id, err := strconv.ParseUint(parts[1], 10, 64); err == nil {
					return id
				}
			}
		}
	}
	
	return 0
}