package wal

import (
	"io"
)

func BeginRecovery(path string, tag []byte) (*WALReader, error) {
	r, err := NewReader(path)
	if err != nil {
		return nil, err
	}

	err = r.SeekTag(tag)
	if err != nil {
		if err != io.EOF {
			r.Close()
			return nil, err
		} else {
			err = r.Reset()
			if err != nil {
				r.Close()
				return nil, err
			}
		}
	}

	return r, nil
}
