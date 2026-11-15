package backfill

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
)

// spillStore is an mst.BlockStore for one downloaded repo. It holds the
// blocks in memory until they exceed threshold bytes, then moves them to a
// temporary file and keeps only a 16-byte index entry per block, so a repo
// far larger than memory can still be loaded, verified and walked.
//
// Like mst.MemBlockStore it is not safe for concurrent use.
type spillStore struct {
	dir       string
	threshold int64

	mem      *mst.MemBlockStore
	memBytes int64

	f     *os.File
	w     *bufio.Writer
	off   int64
	dirty bool

	// Open-addressing table. off holds the record offset plus one, so the
	// zero value marks an empty slot.
	slots []spillSlot
	count int
}

type spillSlot struct{ key, off uint64 }

// spillHeaderLen is a record's fixed prefix: data length, codec, hash.
const spillHeaderLen = 4 + 1 + 32

func newSpillStore(dir string, threshold int64) *spillStore {
	return &spillStore{dir: dir, threshold: threshold, mem: mst.NewMemBlockStore()}
}

// Spilled reports whether the blocks have moved to disk.
func (s *spillStore) Spilled() bool { return s.f != nil }

func spillKey(cid cbor.CID) uint64 {
	h := cid.Hash()
	return binary.LittleEndian.Uint64(h[:8])
}

func (s *spillStore) PutBlock(cid cbor.CID, data []byte) error {
	if s.f == nil {
		if err := s.mem.PutBlock(cid, data); err != nil {
			return err
		}
		s.memBytes += int64(len(data))
		if s.memBytes <= s.threshold {
			return nil
		}
		return s.spill()
	}
	return s.write(cid, data)
}

// spill moves every block held in memory to a new temporary file.
func (s *spillStore) spill() error {
	f, err := os.CreateTemp(s.dir, "repo-*.blocks")
	if err != nil {
		return fmt.Errorf("backfill: spill: %w", err)
	}
	s.f, s.w = f, bufio.NewWriterSize(f, 1<<20)
	s.slots = make([]spillSlot, 1<<16)
	for cid, data := range s.mem.All() {
		if err := s.write(cid, data); err != nil {
			return err
		}
	}
	s.mem = nil
	return nil
}

func (s *spillStore) write(cid cbor.CID, data []byte) error {
	var header [spillHeaderLen]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(len(data)))
	header[4] = uint8(cid.Codec())
	hash := cid.Hash()
	copy(header[5:], hash[:])
	if _, err := s.w.Write(header[:]); err != nil {
		return fmt.Errorf("backfill: spill: %w", err)
	}
	if _, err := s.w.Write(data); err != nil {
		return fmt.Errorf("backfill: spill: %w", err)
	}
	s.insert(spillKey(cid), uint64(s.off)+1)
	s.off += int64(spillHeaderLen + len(data))
	s.dirty = true
	return nil
}

func (s *spillStore) insert(key, off uint64) {
	if (s.count+1)*10 > len(s.slots)*7 {
		old := s.slots
		s.slots = make([]spillSlot, len(old)*2)
		s.count = 0
		for _, slot := range old {
			if slot.off != 0 {
				s.insert(slot.key, slot.off)
			}
		}
	}
	mask := uint64(len(s.slots) - 1)
	for i := key & mask; ; i = (i + 1) & mask {
		if s.slots[i].off == 0 {
			s.slots[i] = spillSlot{key, off}
			s.count++
			return
		}
	}
}

func (s *spillStore) GetBlock(cid cbor.CID) ([]byte, error) {
	if s.f == nil {
		return s.mem.GetBlock(cid)
	}
	if s.dirty {
		if err := s.w.Flush(); err != nil {
			return nil, fmt.Errorf("backfill: spill: %w", err)
		}
		s.dirty = false
	}
	key, hash, codec := spillKey(cid), cid.Hash(), uint8(cid.Codec())
	mask := uint64(len(s.slots) - 1)
	for i := key & mask; ; i = (i + 1) & mask {
		slot := s.slots[i]
		if slot.off == 0 {
			return nil, fmt.Errorf("%w: %s", mst.ErrBlockNotFound, cid.String())
		}
		if slot.key != key {
			continue
		}
		// The table keys on 8 bytes of the hash; confirm the full CID.
		var header [spillHeaderLen]byte
		if _, err := s.f.ReadAt(header[:], int64(slot.off-1)); err != nil {
			return nil, fmt.Errorf("backfill: spill: read block header: %w", err)
		}
		if header[4] != codec || [32]byte(header[5:]) != hash {
			continue
		}
		data := make([]byte, binary.LittleEndian.Uint32(header[:4]))
		if _, err := s.f.ReadAt(data, int64(slot.off-1)+spillHeaderLen); err != nil && !(errors.Is(err, io.EOF) && len(data) == 0) {
			return nil, fmt.Errorf("backfill: spill: read block: %w", err)
		}
		return data, nil
	}
}

// Close removes the temporary file, if any.
func (s *spillStore) Close() error {
	if s.f == nil {
		return nil
	}
	name := s.f.Name()
	err := s.f.Close()
	s.f, s.slots = nil, nil
	return errors.Join(err, os.Remove(name))
}
