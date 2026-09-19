package transport

import (
	"fmt"
	"io/fs"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// RetryAfter parses either form of the HTTP Retry-After header. Invalid or
// overflowing values are errors, never permission to retry earlier.
func RetryAfter(value string, now time.Time) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value != "" {
		digits := true
		for _, r := range value {
			if r < '0' || r > '9' {
				digits = false
				break
			}
		}
		if digits {
			seconds, err := strconv.ParseInt(value, 10, 64)
			if err == nil && seconds <= math.MaxInt64/int64(time.Second) {
				return time.Duration(seconds) * time.Second, nil
			}
			return 0, fmt.Errorf("Retry-After exceeds supported duration: %w", fs.ErrInvalid)
		}
		if at, err := http.ParseTime(value); err == nil {
			delay := at.Sub(now)
			if delay < 0 {
				delay = 0
			}
			return delay, nil
		}
	}
	return 0, fmt.Errorf("invalid Retry-After header: %w", fs.ErrInvalid)
}

func (c *Client) retryWait(attempt int, header string) (time.Duration, bool) {
	delay := min(c.retryDelay, c.maxRetryDelay)
	for i := 0; i < attempt; i++ {
		if delay > c.maxRetryDelay/2 {
			delay = c.maxRetryDelay
			break
		}
		delay *= 2
	}
	if header != "" {
		serverDelay, err := RetryAfter(header, time.Now())
		if err != nil || serverDelay > c.maxRetryDelay {
			return 0, false
		}
		delay = max(delay, serverDelay)
	}
	return delay, true
}
