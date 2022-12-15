//go:build linux
// +build linux

package supportos

import (
	"io/fs"
	"syscall"
	"time"
)

func ItemLocal(fi fs.FileInfo) (time.Time, time.Time, time.Time, uint32, uint32, int64) {
	var atimeLocal, ctimeLocal, mtimeLocal time.Time
	var uid, gid uint32
	var size int64
	if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
		atimeLocal = time.Unix(stat.Atim.Unix()).Local()
		ctimeLocal = time.Unix(stat.Ctim.Unix()).Local()
		mtimeLocal = time.Unix(stat.Mtim.Unix()).Local()
		uid = stat.Uid
		gid = stat.Gid
		size = stat.Size
	}
	return atimeLocal, ctimeLocal, mtimeLocal, uid, gid, size
}
