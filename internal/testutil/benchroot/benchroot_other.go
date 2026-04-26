//go:build !linux

package benchroot

func statFilesystem(path string) (filesystemInfo, error) {
	return filesystemInfo{
		name:     "unknown",
		isMemory: false,
	}, nil
}
