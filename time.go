package dtq

import "time"

func timeUnixNano(n int64) time.Time { return time.Unix(0, n) }
