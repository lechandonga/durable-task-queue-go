package dtq

// 每种记录的第 0 字节是 rec* 类型，其余字段按 record.go 描述的顺序编码。

type enqueueRec struct {
	id         string
	payload    []byte
	enqueuedAt int64
}

func encodeEnqueue(r enqueueRec) []byte {
	e := enc{}
	e.bytev(recEnqueue)
	e.str(r.id)
	e.bytes(r.payload)
	e.i64(r.enqueuedAt)
	return e.b
}

func decodeEnqueue(p []byte) (enqueueRec, bool) {
	d := dec{b: p[1:]}
	r := enqueueRec{id: d.str(), payload: d.bytes(), enqueuedAt: d.i64()}
	return r, d.err == nil && len(d.b) == 0
}

type leasedRec struct {
	id          string
	delivery    uint64
	attempts    int
	leaseExpire int64
	timeoutMS   int64
}

func encodeLeased(r leasedRec) []byte {
	e := enc{}
	e.bytev(recLeased)
	e.str(r.id)
	e.u64(r.delivery)
	e.u32(uint32(r.attempts))
	e.i64(r.leaseExpire)
	e.i64(r.timeoutMS)
	return e.b
}

func decodeLeased(p []byte) (leasedRec, bool) {
	d := dec{b: p[1:]}
	r := leasedRec{
		id:          d.str(),
		delivery:    d.u64(),
		attempts:    int(d.u32()),
		leaseExpire: d.i64(),
		timeoutMS:   d.i64(),
	}
	return r, d.err == nil && len(d.b) == 0
}

type ackedRec struct {
	id       string
	delivery uint64
	at       int64
}

func encodeAcked(r ackedRec) []byte {
	e := enc{}
	e.bytev(recAcked)
	e.str(r.id)
	e.u64(r.delivery)
	e.i64(r.at)
	return e.b
}

func decodeAcked(p []byte) (ackedRec, bool) {
	d := dec{b: p[1:]}
	r := ackedRec{id: d.str(), delivery: d.u64(), at: d.i64()}
	return r, d.err == nil && len(d.b) == 0
}

type nackedRec struct {
	id        string
	delivery  uint64
	attempts  int
	retryable bool
	reason    string
	at        int64
}

func encodeNacked(r nackedRec) []byte {
	e := enc{}
	e.bytev(recNacked)
	e.str(r.id)
	e.u64(r.delivery)
	e.u32(uint32(r.attempts))
	if r.retryable {
		e.bytev(1)
	} else {
		e.bytev(0)
	}
	e.str(r.reason)
	e.i64(r.at)
	return e.b
}

func decodeNacked(p []byte) (nackedRec, bool) {
	d := dec{b: p[1:]}
	r := nackedRec{
		id:       d.str(),
		delivery: d.u64(),
		attempts: int(d.u32()),
	}
	if v := d.bytev(); v == 1 {
		r.retryable = true
	} else if v == 0 {
		r.retryable = false
	} else {
		d.err = errCorruptWAL
	}
	r.reason = d.str()
	r.at = d.i64()
	return r, d.err == nil && len(d.b) == 0
}

type deadRec struct {
	id         string
	attempts   int
	reason     string
	enqueuedAt int64
	deadAt     int64
}

func encodeDead(r deadRec) []byte {
	e := enc{}
	e.bytev(recDead)
	e.str(r.id)
	e.u32(uint32(r.attempts))
	e.str(r.reason)
	e.i64(r.enqueuedAt)
	e.i64(r.deadAt)
	return e.b
}

func decodeDead(p []byte) (deadRec, bool) {
	d := dec{b: p[1:]}
	r := deadRec{
		id:         d.str(),
		attempts:   int(d.u32()),
		reason:     d.str(),
		enqueuedAt: d.i64(),
		deadAt:     d.i64(),
	}
	return r, d.err == nil && len(d.b) == 0
}

type leaseReleaseRec struct {
	id       string
	delivery uint64
}

func encodeLeaseRelease(r leaseReleaseRec) []byte {
	e := enc{}
	e.bytev(recLeaseRelease)
	e.str(r.id)
	e.u64(r.delivery)
	return e.b
}

func decodeLeaseRelease(p []byte) (leaseReleaseRec, bool) {
	d := dec{b: p[1:]}
	r := leaseReleaseRec{id: d.str(), delivery: d.u64()}
	return r, d.err == nil && len(d.b) == 0
}

// snapshotData 是一次压缩快照的全部内容。
type snapshotData struct {
	at    int64
	tasks []*taskState
	dead  []DeadLetter
}

// encodeSnapshot 把全部存活状态编成一帧：
//
//	recSnapshot | at:i64
//	taskCount:u32
//	每个任务: status:u8 id:str payload:bytes enqueuedAt:i64
//	          delivery:u64 attempts:u32 leaseExpire:i64 timeoutMS:i64 due:i64
//	deadCount:u32
//	每条死信: id:str payload:bytes attempts:u32 reason:str
//	          enqueuedAt:i64 deadAt:i64
func encodeSnapshot(s snapshotData) []byte {
	e := enc{}
	e.bytev(recSnapshot)
	e.i64(s.at)
	e.u32(uint32(len(s.tasks)))
	for _, t := range s.tasks {
		e.bytev(t.status)
		e.str(t.id)
		e.bytes(t.payload)
		e.i64(t.enqueuedAt)
		e.u64(t.delivery)
		e.u32(uint32(t.attempts))
		e.i64(t.leaseExpire)
		e.i64(t.timeoutMS)
		e.i64(t.due)
	}
	e.u32(uint32(len(s.dead)))
	for _, d := range s.dead {
		e.str(d.ID)
		e.bytes(d.Payload)
		e.u32(uint32(d.Attempts))
		e.str(d.Reason)
		e.i64(d.EnqueuedAt.UnixNano())
		e.i64(d.DeadAt.UnixNano())
	}
	return e.b
}

func decodeSnapshot(p []byte) (snapshotData, bool) {
	d := dec{b: p[1:]}
	s := snapshotData{at: d.i64()}
	nt := int(d.u32())
	for i := 0; i < nt; i++ {
		t := &taskState{heapIdx: -1}
		t.status = d.bytev()
		t.id = d.str()
		t.payload = d.bytes()
		t.enqueuedAt = d.i64()
		t.delivery = d.u64()
		t.attempts = int(d.u32())
		t.leaseExpire = d.i64()
		t.timeoutMS = d.i64()
		t.due = d.i64()
		if d.err != nil {
			return s, false
		}
		s.tasks = append(s.tasks, t)
	}
	nd := int(d.u32())
	for i := 0; i < nd; i++ {
		var dl DeadLetter
		dl.ID = d.str()
		dl.Payload = d.bytes()
		dl.Attempts = int(d.u32())
		dl.Reason = d.str()
		dl.EnqueuedAt = timeUnixNano(d.i64())
		dl.DeadAt = timeUnixNano(d.i64())
		if d.err != nil {
			return s, false
		}
		s.dead = append(s.dead, dl)
	}
	return s, d.err == nil && len(d.b) == 0
}
