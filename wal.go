package wal

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type WriteOptions struct {
	// The maximum size in bytes of each segment. When it reaches near this size,
	// a new segment will be created.
	SegmentSize int64

	// The maximum number of segments to keep on disk.
	MaxSegments int

	// The maximum time of segments to keep on disk.
	SegmentTTL time.Duration

	// If 0, sync is done after every write. Otherwise this controls
	// how often the WAL is sync'd to disk. Setting this can speed
	// up the WAL by sacrifing safety.
	SyncRate time.Duration
}

const MaxSegmentSize = 16 * (1024 * 1024)

// Defaults to using 160MB of disk
var DefaultWriteOptions = WriteOptions{
	SegmentSize: MaxSegmentSize,
	MaxSegments: 10,
}

// Calculate the WriteOptions based on how much disk space the WAL
// should consume in total. The true on disk size might be more
// slightly more than this because the value is calculate against
// MaxSegmentSize, which is 16MB. If you wish to use a larger segment
// size (or more accurate one), then set SegmentSize and MaxSegments
// directly.
func (wo *WriteOptions) CalculateFromTotal(total int64) {
	if wo.MaxSegments == 0 {
		switch {
		case total < MaxSegmentSize:
			wo.MaxSegments = 1
			wo.SegmentSize = total
		default:
			wo.SegmentSize = MaxSegmentSize

			segments := total / wo.SegmentSize

			// Round up, not down.
			if total%wo.SegmentSize != 0 {
				segments++
			}

			wo.MaxSegments = int(segments)
		}
	} else {
		wo.SegmentSize = total / int64(wo.MaxSegments)
	}
}

type tagCache struct {
	Tags map[string]Position `json:"tags"`
}

type WALWriter struct {
	opts WriteOptions

	lock    sync.Mutex
	root    string
	current string

	first int
	index int

	segment *SegmentWriter

	cache     tagCache
	cacheFile *os.File
	cacheEnc  *json.Encoder
}

func rangeSegments(path string) (int, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}

	defer f.Close()

	files, err := f.Readdirnames(-1)
	if err != nil {
		return 0, 0, err
	}

	var (
		first = -1
		last  = -1
	)

	for _, file := range files {
		i, err := strconv.Atoi(file)
		if err == nil {
			if first == -1 || i < first {
				first = i
			}

			if last == -1 || i > last {
				last = i
			}
		}
	}

	return first, last, nil
}

func New(root string) (*WALWriter, error) {
	return NewWithOptions(root, DefaultWriteOptions)
}

func NewWithOptions(root string, opts WriteOptions) (*WALWriter, error) {
	err := os.Mkdir(root, 0755)
	if err != nil {
		if !os.IsExist(err) {
			return nil, err
		}
	}

	first, last, err := rangeSegments(root)
	if err != nil {
		return nil, err
	}

	if last == -1 {
		last = 0
	}

	if first == -1 {
		first = 0
	}

	cache, err := os.Create(filepath.Join(root, "tags"))
	if err != nil {
		return nil, err
	}

	wal := &WALWriter{
		root:      root,
		current:   filepath.Join(root, fmt.Sprintf("%d", last)),
		first:     first,
		index:     last,
		opts:      opts,
		cacheFile: cache,
		cacheEnc:  json.NewEncoder(cache),
	}

	wal.cache.Tags = make(map[string]Position)

	seg, err := NewSegmentWriter(wal.current)
	if err != nil {
		return nil, err
	}

	wal.segment = seg

	if opts.SyncRate > 0 {
		seg.SetSyncRate(opts.SyncRate)
	}

	return wal, nil
}

func (wal *WALWriter) rotateSegment() error {
	err := wal.segment.Close()
	if err != nil {
		return err
	}

	wal.index++

	wal.current = filepath.Join(wal.root, fmt.Sprintf("%d", wal.index))

	seg, err := NewSegmentWriter(wal.current)
	if err != nil {
		return err
	}

	wal.segment = seg

	return nil
}

func (wal *WALWriter) pruneSegments(total int, expiration time.Time) error {
	startAt := wal.index - total + 1
	if startAt < wal.first {
		startAt = wal.first
	}

	if !expiration.IsZero() {
		for ; startAt < wal.index; startAt++ {
			filePath := filepath.Join(wal.root, fmt.Sprintf("%d", startAt))
			stat, err := os.Stat(filePath)
			if err != nil {
				if !os.IsNotExist(err) {
					return err
				}
			} else if stat.ModTime().After(expiration) {
				// larger number means newer
				// no need to check next if this is after expiration
				break
			}
		}
	}

	pruned := false
	for i := startAt - 1; i >= wal.first; i-- {
		err := os.Remove(filepath.Join(wal.root, fmt.Sprintf("%d", i)))
		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		}
		pruned = true
	}

	if pruned {
		// Move the oldest horizon forward to our current first segment
		wal.first = startAt
		pruned = false
		// remove tag cache as well
		for tag, pos := range wal.cache.Tags {
			if pos.Segment < startAt {
				delete(wal.cache.Tags, tag)
				pruned = true
			}
		}
		if pruned {
			err := wal.flushTagsFile()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

const averageOverhead = 4 + 1 + 2

func (wal *WALWriter) Write(data []byte) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	newSize := int64(len(data)) + averageOverhead + wal.segment.Size()

	if newSize > wal.opts.SegmentSize {
		err := wal.rotateSegment()
		if err != nil {
			return err
		}

		var expiration time.Time
		if wal.opts.SegmentTTL != 0 {
			expiration = time.Now().Add(-wal.opts.SegmentTTL)
		}
		err = wal.pruneSegments(wal.opts.MaxSegments, expiration)
		if err != nil {
			return err
		}
	}

	_, err := wal.segment.Write(data)
	return err
}

type Position struct {
	Segment int   `json:"segment"`
	Offset  int64 `json:"offset"`
}

func (p *Position) None() bool {
	return p.Segment == -1
}

func (wal *WALWriter) Pos() (Position, error) {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	pos := wal.segment.Pos()

	return Position{wal.index, pos}, nil
}

func (wal *WALWriter) flushTagsFile() error {
	err := wal.cacheFile.Truncate(0)
	if err != nil {
		return err
	}
	_, err = wal.cacheFile.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}
	err = wal.cacheEnc.Encode(&wal.cache)
	if err != nil {
		return err
	}

	return wal.cacheFile.Sync()
}

func (wal *WALWriter) WriteTag(tag []byte) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	// We truncate the cache and rewrite it after the segment
	// has confirmed the tag so the cache is either absent
	// or correct, never present but out of date.

	segPos := wal.segment.Pos()

	err := wal.segment.WriteTag(tag)
	if err != nil {
		return err
	}
	wal.cache.Tags[string(tag)] = Position{wal.index, segPos}

	return wal.flushTagsFile()
}

func (wal *WALWriter) Close() error {
	return wal.segment.Close()
}

type WALReader struct {
	root    string
	current string

	first int
	last  int
	index int

	seg *SegmentReader

	err error
}

var ErrNoSegments = errors.New("no segments")

func NewReader(root string) (*WALReader, error) {
	r := &WALReader{root: root}

	err := r.Reset()
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (wal *WALReader) Reset() error {
	if wal.seg != nil {
		wal.seg.Close()
	}

	first, last, err := rangeSegments(wal.root)
	if err != nil {
		return err
	}

	if first == -1 {
		return ErrNoSegments
	}

	cur := filepath.Join(wal.root, fmt.Sprintf("%d", first))

	r, err := NewSegmentReader(cur)
	if err != nil {
		return err
	}

	wal.current = cur
	wal.first = first
	wal.last = last
	wal.index = first
	wal.seg = r

	return nil
}

func (wal *WALReader) Pos() Position {
	if wal.err != nil || wal.seg == nil {
		return Position{-1, -1}
	}
	return Position{wal.index, wal.seg.Pos()}
}

func (wal *WALReader) Seek(p Position) error {
	if p.Segment == wal.index && wal.seg != nil {
		return wal.seg.Seek(p.Offset)
	}
	path := filepath.Join(wal.root, fmt.Sprintf("%d", p.Segment))

	seg, err := NewSegmentReader(path)
	if err != nil {
		return err
	}

	err = seg.Seek(p.Offset)
	if err != nil {
		return err
	}

	if wal.seg != nil {
		wal.seg.Close()
	}

	wal.index = p.Segment
	wal.seg = seg

	return nil
}

func (wal *WALReader) SeekLast() error {
	p1 := Position{
		Segment: -1,
		Offset:  -1,
	}
	p2 := Position{
		Segment: wal.last,
		Offset:  0,
	}
	err := wal.Seek(p2)
	if err != nil {
		return err
	}
	for wal.Next() {
		p1 = p2
		p2 = wal.Pos()
	}
	err = wal.Error()
	if err != nil {
		return err
	}
	if p1.None() {
		return ErrNoSegments
	}
	return wal.Seek(p1)
}

func (wal *WALReader) SeekTag(tag []byte) error {
	cacheFile, err := os.Open(filepath.Join(wal.root, "tags"))
	if err == nil {
		defer cacheFile.Close()
		var cache tagCache
		err = json.NewDecoder(cacheFile).Decode(&cache)
		if err != nil {
			return err
		}
		if pos, found := cache.Tags[string(tag)]; found {
			err = wal.Seek(pos)
			if err != nil {
				return err
			}
			if wal.next(tagType) {
				if bytes.Equal(wal.Value(), tag) {
					return nil
				}
			}
			goto eof
		}
	} else {
		// TODO: warning
	}

	for wal.next(tagType) {
		if bytes.Equal(wal.Value(), tag) {
			return nil
		}
	}
eof:
	err = wal.Error()
	if err != nil {
		return err
	}
	return io.EOF
}

func (r *WALReader) Close() error {
	if r.seg == nil {
		return nil
	}

	return r.seg.Close()
}

func (r *WALReader) Next() bool {
	return r.next(dataType)
}

func (r *WALReader) next(typ byte) bool {
	r.err = nil
	if r.seg != nil && r.seg.next(typ) {
		return true
	}

	idx := r.index
	for {
		idx++
		if idx > r.last {
			_, last, err := rangeSegments(r.root)
			if err != nil {
				r.err = err
				return false
			}
			r.last = last
			if idx > r.last {
				return false
			}
		}

		r.index = idx

		path := filepath.Join(r.root, fmt.Sprintf("%d", r.index))

		seg, err := NewSegmentReader(path)
		if err != nil {
			r.err = err
			return false
		}

		if r.seg != nil {
			r.seg.Close()
		}
		r.seg = seg
		if r.seg.next(typ) {
			break
		} else {
			return false
		}
	}

	return true
}

func (r *WALReader) Value() []byte {
	if r.seg == nil {
		return nil
	}

	return r.seg.Value()
}

func (r *WALReader) Error() error {
	if r.err != nil {
		return r.err
	}

	if r.seg != nil {
		return r.seg.Error()
	}

	return nil
}
