package dtq

import (
	"encoding/binary"
	"hash/crc32"
)

// 文件格式（版本 1）
//
// 文件头：
//   magic   = "DTQ1"          4 字节
//   version = uint16 LE        2 字节（当前为 1）
//   flags   = uint16 LE        2 字节（保留，必须为 0）
//
// 文件头之后是一串长度前缀帧，帧之间没有填充：
//   payloadLen = uint32 LE     4 字节
//   payload    = payloadLen 字节
//   crc32(IEEE, payload)       4 字节
//
// 所有状态变更都是只追加的；进程崩溃只会在文件末尾留下不完整的帧，
// 恢复时从最后一个校验通过的帧之后截断即可，不会读到脏数据。

const (
	magic0 = 'D'
	magic1 = 'T'
	magic2 = 'Q'
	magic3 = '1'

	formatVersion1 uint16 = 1
	currentVersion        = formatVersion1

	headerSize = 8

	// 单条记录负载上限，防止损坏的长度字段导致巨量分配。
	maxPayloadSize = 64 * 1024 * 1024
)

// 记录类型。新增类型只能递增分配，不得复用旧编号。
const (
	recEnqueue      byte = 1 // 新任务入队
	recLeased       byte = 2 // 任务被某个消费者持有
	recAcked        byte = 3 // 任务处理成功并确认
	recNacked       byte = 4 // 任务处理失败（可重试 / 不可重试）
	recDead         byte = 5 // 任务进入死信
	recSnapshot     byte = 6 // 压缩快照，恢复时清空此前状态
	recLeaseRelease byte = 7 // 正常关闭时在途任务被放回可投递队列
)

func fileHeader() []byte {
	h := make([]byte, headerSize)
	h[0], h[1], h[2], h[3] = magic0, magic1, magic2, magic3
	binary.LittleEndian.PutUint16(h[4:6], currentVersion)
	return h
}

// frame 把一条记录编码成一个完整磁盘帧。
func frame(payload []byte) []byte {
	out := make([]byte, 4+len(payload)+4)
	binary.LittleEndian.PutUint32(out, uint32(len(payload)))
	copy(out[4:], payload)
	binary.LittleEndian.PutUint32(out[4+len(payload):], crc32.ChecksumIEEE(payload))
	return out
}

type enc struct{ b []byte }

func (e *enc) bytev(v byte)   { e.b = append(e.b, v) }
func (e *enc) u16(v uint16)   { e.b = binary.LittleEndian.AppendUint16(e.b, v) }
func (e *enc) u32(v uint32)   { e.b = binary.LittleEndian.AppendUint32(e.b, v) }
func (e *enc) u64(v uint64)   { e.b = binary.LittleEndian.AppendUint64(e.b, v) }
func (e *enc) i64(v int64)    { e.b = binary.LittleEndian.AppendUint64(e.b, uint64(v)) }
func (e *enc) str(s string)   { e.bytes([]byte(s)) }
func (e *enc) bytes(p []byte) { e.u32(uint32(len(p))); e.b = append(e.b, p...) }

type dec struct {
	b   []byte
	err error
}

func (d *dec) bytev() byte {
	if d.err != nil || len(d.b) < 1 {
		d.err = errCorruptWAL
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

func (d *dec) u16() uint16 {
	if d.err != nil || len(d.b) < 2 {
		d.err = errCorruptWAL
		return 0
	}
	v := binary.LittleEndian.Uint16(d.b)
	d.b = d.b[2:]
	return v
}

func (d *dec) u32() uint32 {
	if d.err != nil || len(d.b) < 4 {
		d.err = errCorruptWAL
		return 0
	}
	v := binary.LittleEndian.Uint32(d.b)
	d.b = d.b[4:]
	return v
}

func (d *dec) u64() uint64 {
	if d.err != nil || len(d.b) < 8 {
		d.err = errCorruptWAL
		return 0
	}
	v := binary.LittleEndian.Uint64(d.b)
	d.b = d.b[8:]
	return v
}

func (d *dec) i64() int64 { return int64(d.u64()) }

func (d *dec) str() string { return string(d.bytes()) }

func (d *dec) bytes() []byte {
	n := int(d.u32())
	if d.err != nil {
		return nil
	}
	if n < 0 || n > len(d.b) {
		d.err = errCorruptWAL
		return nil
	}
	p := d.b[:n]
	d.b = d.b[n:]
	return p
}
