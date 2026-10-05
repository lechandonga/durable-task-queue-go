package dtq

// dueHeap 按 due 时间组织任务：ready 任务 due=0 永远排在最前，
// leased/delayed 任务按到期时刻排列。相同 due 时按入队顺序（map 迭代无关，
// 投递顺序由调用方在 lease 时按 ready 切片保证 FIFO）。
type dueHeap []*taskState

func (h dueHeap) Len() int { return len(h) }

func (h dueHeap) Less(i, j int) bool {
	if h[i].due != h[j].due {
		return h[i].due < h[j].due
	}
	return h[i].enqueuedAt < h[j].enqueuedAt
}

func (h dueHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx, h[j].heapIdx = i, j
}

func (h *dueHeap) Push(x any) {
	t := x.(*taskState)
	t.heapIdx = len(*h)
	*h = append(*h, t)
}

func (h *dueHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	x.heapIdx = -1
	return x
}
