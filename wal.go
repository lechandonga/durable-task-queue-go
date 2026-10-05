package dtq

import (
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// walFile 封装队列数据文件。
//
// 文件在 Open 时校验文件头与版本；帧的完整性在读取时靠长度 + CRC 保证。
// 写入永远只发生在文件末尾，崩溃只会截断最后一个帧，文件中间不可能
// 出现空洞：因此恢复时遇到第一个坏帧就可以截断后续所有内容。
type walFile struct {
	path string
	f    *os.File
}

func openWAL(dir string) (*walFile, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "queue.log")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	w := &walFile{path: path, f: f}
	if err := w.initHeader(); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

func (w *walFile) initHeader() error {
	buf := make([]byte, headerSize)
	n, err := io.ReadFull(w.f, buf)
	if errors.Is(err, io.EOF) {
		// 全新的空文件：写入文件头。
		if _, werr := w.f.Write(fileHeader()); werr != nil {
			return werr
		}
		return w.f.Sync()
	}
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return err
	}
	if n < 4 || buf[0] != magic0 || buf[1] != magic1 || buf[2] != magic2 {
		return errCorruptWAL
	}
	if n < headerSize {
		// 文件头本身就写了一半：无法判断版本，拒绝处理且不动文件。
		return errCorruptWAL
	}
	v := uint16(buf[4]) | uint16(buf[5])<<8
	if v != formatVersion1 {
		// 显式拒绝未知版本，绝不覆盖原有数据。
		return ErrUnknownVersion
	}
	if buf[3] != '0'+byte(v) || buf[6] != 0 || buf[7] != 0 {
		return errCorruptWAL
	}
	return nil
}

// scan 按顺序回调每个校验通过的帧。遇到第一个不完整/损坏帧时
// 返回该帧在文件中的起始偏移（validEnd），调用方可据此截断。
// frame 中间出现坏数据也返回 validEnd，因为只追加语义保证其后全部不可信。
func (w *walFile) scan(visit func(payload []byte) error) (validEnd int64, err error) {
	if _, err := w.f.Seek(headerSize, io.SeekStart); err != nil {
		return 0, err
	}
	var lenBuf [4]byte
	var crcBuf [4]byte
	off := int64(headerSize)
	for {
		if _, err := io.ReadFull(w.f, lenBuf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return off, nil // 干净结束
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return off, errCorruptWAL // 长度字段残缺
			}
			return off, err
		}
		n := uint32(lenBuf[0]) | uint32(lenBuf[1])<<8 | uint32(lenBuf[2])<<16 | uint32(lenBuf[3])<<24
		if n > maxPayloadSize {
			return off, errCorruptWAL
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(w.f, payload); err != nil {
			return off, errCorruptWAL
		}
		if _, err := io.ReadFull(w.f, crcBuf[:]); err != nil {
			return off, errCorruptWAL
		}
		got := uint32(crcBuf[0]) | uint32(crcBuf[1])<<8 | uint32(crcBuf[2])<<16 | uint32(crcBuf[3])<<24
		if got != crc32.ChecksumIEEE(payload) {
			return off, errCorruptWAL
		}
		if err := visit(payload); err != nil {
			return off, err
		}
		off += int64(4 + n + 4)
	}
}

func (w *walFile) appendRecord(payload []byte) error {
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	if _, err := w.f.Write(frame(payload)); err != nil {
		return err
	}
	return w.f.Sync()
}

// truncateTail 丢弃 off 之后的残缺数据。
func (w *walFile) truncateTail(off int64) error {
	if err := w.f.Truncate(off); err != nil {
		return err
	}
	return w.f.Sync()
}

// rewrite 用新内容原子替换整个数据文件（用于压缩）。
func (w *walFile) rewrite(full []byte) error {
	tmp := w.path + ".tmp"
	tf, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := tf.Write(full); err != nil {
		tf.Close()
		return err
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		return err
	}
	if err := tf.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, w.path); err != nil {
		return err
	}
	// fsync 目录，保证 rename 在崩溃后也持久化。
	if dir, derr := os.Open(filepath.Dir(w.path)); derr == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	f, err := os.OpenFile(w.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	old := w.f
	w.f = f
	return old.Close()
}

func (w *walFile) close() error { return w.f.Close() }
