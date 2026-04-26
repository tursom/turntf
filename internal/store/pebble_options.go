package store

import (
	"runtime"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

const (
	pebbleThroughputCacheSize               = 128 << 20
	pebbleThroughputMemTableSize            = 64 << 20
	pebbleThroughputMaxOpenFiles            = 16384
	pebbleThroughputBytesPerSync            = 1 << 20
	pebbleThroughputWALBytesPerSync         = 1 << 20
	pebbleThroughputInlineValueMaxBytes     = 1024
	pebbleThroughputL0StopWritesThreshold   = 20
	pebbleThroughputWALMinSyncIntervalDelay = 500 * time.Microsecond
)

func openPebbleDB(path string, profile PebbleProfile) (*pebble.DB, error) {
	opts, release := newPebbleOptions(profile)
	if release != nil {
		defer release()
	}
	return pebble.Open(path, opts)
}

func newPebbleOptions(profile PebbleProfile) (*pebble.Options, func()) {
	if profile != PebbleProfileThroughput {
		return &pebble.Options{}, nil
	}

	cache := pebble.NewCache(pebbleThroughputCacheSize)
	opts := &pebble.Options{
		Cache:                       cache,
		FormatMajorVersion:          pebble.FormatNewest,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       pebbleThroughputL0StopWritesThreshold,
		LBaseMaxBytes:               64 << 20,
		MaxOpenFiles:                pebbleThroughputMaxOpenFiles,
		MemTableSize:                pebbleThroughputMemTableSize,
		MemTableStopWritesThreshold: 4,
		BytesPerSync:                pebbleThroughputBytesPerSync,
		WALBytesPerSync:             pebbleThroughputWALBytesPerSync,
		WALMinSyncInterval: func() time.Duration {
			return pebbleThroughputWALMinSyncIntervalDelay
		},
		MaxConcurrentCompactions: func() int {
			procs := runtime.GOMAXPROCS(0)
			switch {
			case procs >= 8:
				return 3
			case procs >= 4:
				return 2
			default:
				return 1
			}
		},
		Levels: make([]pebble.LevelOptions, 7),
	}
	for i := range opts.Levels {
		level := &opts.Levels[i]
		level.BlockSize = 32 << 10
		level.IndexBlockSize = 256 << 10
		level.Compression = pebble.NoCompression
		level.FilterPolicy = bloom.FilterPolicy(10)
		level.FilterType = pebble.TableFilter
		if i > 0 {
			level.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
		level.EnsureDefaults()
	}
	opts.Levels[6].FilterPolicy = nil
	opts.FlushSplitBytes = opts.Levels[0].TargetFileSize
	opts.EnsureDefaults()
	return opts, func() {
		cache.Unref()
	}
}
