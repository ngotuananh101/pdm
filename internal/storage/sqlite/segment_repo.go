// SQLite implementation of domain.SegmentRepository (Stage 3, Phase 1.3).
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// SegmentRepository is the SQLite implementation of
// domain.SegmentRepository.
//
// Mapping (schema 001, ARCH §5): end_offset is inclusive (NULL only
// for a segment of unknown size, which no Stage 3 writer creates;
// a NULL end maps back to -1); status is "pending", "running" or
// "complete"; committed_bytes only ever moves forward.
type SegmentRepository struct {
	db *sql.DB
}

// NewSegmentRepository returns a repository backed by db. The caller
// keeps ownership of db (open it with Open, close it when done).
func NewSegmentRepository(db *sql.DB) *SegmentRepository {
	return &SegmentRepository{db: db}
}

var _ domain.SegmentRepository = (*SegmentRepository)(nil)

// segmentColumns is the persisted-field column list, in scan order.
const segmentColumns = `download_id, "index", start_offset, end_offset, committed_bytes, status`

// scanSegment reads one segment row in segmentColumns order. A NULL
// end_offset (unknown size) maps to EndOffset -1.
func scanSegment(row rowScanner) (*domain.Segment, error) {
	var s domain.Segment
	var status string
	var end sql.NullInt64
	if err := row.Scan(
		&s.DownloadID, &s.Index, &s.StartOffset, &end, &s.CommittedBytes, &status,
	); err != nil {
		return nil, err
	}
	if end.Valid {
		s.EndOffset = end.Int64
	} else {
		s.EndOffset = -1
	}
	s.Status = domain.SegmentStatus(status)
	return &s, nil
}

// segmentNotFound wraps domain.ErrSegmentNotFound with the addressed
// download and index.
func segmentNotFound(downloadID string, index int) error {
	return fmt.Errorf("sqlite: segment %q[%d]: %w", downloadID, index, domain.ErrSegmentNotFound)
}

func (r *SegmentRepository) dbOrErr() (*sql.DB, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("sqlite: SegmentRepository used with nil DB")
	}
	return r.db, nil
}

func validSegmentStatus(s domain.SegmentStatus) bool {
	switch s {
	case domain.SegmentPending, domain.SegmentRunning, domain.SegmentComplete:
		return true
	default:
		return false
	}
}

// validatePlanSegment checks one segment of a plan before insert:
// offsets must form a non-empty range, committed bytes must lie
// inside the range, and the status must be a known value.
func validatePlanSegment(s domain.Segment) error {
	if s.Index < 0 {
		return fmt.Errorf("sqlite: segment index %d: negative", s.Index)
	}
	if s.StartOffset < 0 || s.EndOffset < s.StartOffset {
		return fmt.Errorf("sqlite: segment %d: invalid range [%d, %d]", s.Index, s.StartOffset, s.EndOffset)
	}
	if s.CommittedBytes < 0 || s.CommittedBytes > s.Size() {
		return fmt.Errorf("sqlite: segment %d: committed_bytes %d outside [0, %d]", s.Index, s.CommittedBytes, s.Size())
	}
	if !validSegmentStatus(s.Status) {
		return fmt.Errorf("sqlite: segment %d: invalid status %q", s.Index, s.Status)
	}
	return nil
}

// InsertPlan stores every segment of one download in a single
// transaction: either the whole plan lands or none of it does.
func (r *SegmentRepository) InsertPlan(ctx context.Context, downloadID string, segments []domain.Segment) error {
	db, err := r.dbOrErr()
	if err != nil {
		return err
	}
	if downloadID == "" {
		return fmt.Errorf("sqlite: InsertPlan called with empty download ID")
	}
	if len(segments) == 0 {
		return fmt.Errorf("sqlite: InsertPlan %q: empty segment plan", downloadID)
	}
	for _, s := range segments {
		if err := validatePlanSegment(s); err != nil {
			return err
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: InsertPlan %q: begin: %w", downloadID, err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, s := range segments {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO segments (`+segmentColumns+`) VALUES (?, ?, ?, ?, ?, ?)`,
			downloadID, s.Index, s.StartOffset, s.EndOffset, s.CommittedBytes, string(s.Status),
		); err != nil {
			return fmt.Errorf("sqlite: InsertPlan %q: segment %d: %w", downloadID, s.Index, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: InsertPlan %q: commit: %w", downloadID, err)
	}
	return nil
}

// GetByDownload returns the segments of one download in index order.
func (r *SegmentRepository) GetByDownload(ctx context.Context, downloadID string) ([]*domain.Segment, error) {
	db, err := r.dbOrErr()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx,
		`SELECT `+segmentColumns+` FROM segments WHERE download_id = ? ORDER BY "index" ASC`, downloadID)
	if err != nil {
		return nil, fmt.Errorf("sqlite: GetByDownload %q: %w", downloadID, err)
	}
	defer rows.Close()
	out := []*domain.Segment{}
	for rows.Next() {
		s, err := scanSegment(rows)
		if err != nil {
			return nil, fmt.Errorf("sqlite: GetByDownload %q: scan: %w", downloadID, err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: GetByDownload %q: %w", downloadID, err)
	}
	return out, nil
}

// UpdateCommitted advances the committed_bytes of one segment. The
// value must not exceed the segment size and must never go below the
// stored value; a backwards update is refused and leaves the stored
// value unchanged. The read and the write run in one transaction.
func (r *SegmentRepository) UpdateCommitted(ctx context.Context, downloadID string, index int, committedBytes int64) error {
	db, err := r.dbOrErr()
	if err != nil {
		return err
	}
	if committedBytes < 0 {
		return fmt.Errorf("sqlite: UpdateCommitted %q[%d]: negative committed_bytes %d", downloadID, index, committedBytes)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: UpdateCommitted %q[%d]: begin: %w", downloadID, index, err)
	}
	defer func() { _ = tx.Rollback() }()
	var current int64
	var start, end sql.NullInt64
	if err := tx.QueryRowContext(ctx,
		`SELECT committed_bytes, start_offset, end_offset FROM segments WHERE download_id = ? AND "index" = ?`,
		downloadID, index,
	).Scan(&current, &start, &end); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return segmentNotFound(downloadID, index)
		}
		return fmt.Errorf("sqlite: UpdateCommitted %q[%d]: read: %w", downloadID, index, err)
	}
	if committedBytes < current {
		return fmt.Errorf("sqlite: UpdateCommitted %q[%d]: %d < stored %d: %w",
			downloadID, index, committedBytes, current, domain.ErrCommittedBytesRegression)
	}
	if end.Valid && committedBytes > end.Int64-start.Int64+1 {
		return fmt.Errorf("sqlite: UpdateCommitted %q[%d]: committed_bytes %d exceeds segment size %d",
			downloadID, index, committedBytes, end.Int64-start.Int64+1)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE segments SET committed_bytes = ? WHERE download_id = ? AND "index" = ?`,
		committedBytes, downloadID, index,
	); err != nil {
		return fmt.Errorf("sqlite: UpdateCommitted %q[%d]: %w", downloadID, index, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: UpdateCommitted %q[%d]: commit: %w", downloadID, index, err)
	}
	return nil
}

// MarkStatus changes only the status of one segment.
func (r *SegmentRepository) MarkStatus(ctx context.Context, downloadID string, index int, status domain.SegmentStatus) error {
	db, err := r.dbOrErr()
	if err != nil {
		return err
	}
	if !validSegmentStatus(status) {
		return fmt.Errorf("sqlite: MarkStatus %q[%d]: invalid status %q", downloadID, index, status)
	}
	res, err := db.ExecContext(ctx,
		`UPDATE segments SET status = ? WHERE download_id = ? AND "index" = ?`,
		string(status), downloadID, index)
	if err != nil {
		return fmt.Errorf("sqlite: MarkStatus %q[%d]: %w", downloadID, index, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: MarkStatus %q[%d]: rows affected: %w", downloadID, index, err)
	}
	if n == 0 {
		return segmentNotFound(downloadID, index)
	}
	return nil
}

// DeleteByDownload removes every segment of one download. Deleting
// segments that do not exist is a no-op, not an error.
func (r *SegmentRepository) DeleteByDownload(ctx context.Context, downloadID string) error {
	db, err := r.dbOrErr()
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM segments WHERE download_id = ?`, downloadID); err != nil {
		return fmt.Errorf("sqlite: DeleteByDownload %q: %w", downloadID, err)
	}
	return nil
}
