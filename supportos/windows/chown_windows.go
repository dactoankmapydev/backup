//go:build windows
// +build windows

package supportos

func SetChownItem(name string, uid int, gid int) error {
	return nil
}
