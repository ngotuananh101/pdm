// SQLite implementation of domain.DownloadRepository (Phase 2.2/2.3).
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// DownloadRepository is the SQLite implementation of
// domain.DownloadRepository.
//
// It owns queue_position: Insert appends at max + 1, ListAll orders by
// it, and Move rewrites it. queue_position is deliberately not a field
// of domain.Download (see the interface doc in internal/domain/ports.go).
// Update never touches queue_position.
//
// Mapping (schema 001, ARCH §5): TotalBytes -1 <-> NULL, empty
// ETag / LastModified <-> NULL, SupportsRange <-> 0/1. Timestamps are
// stored as UTC RFC 3339 text. SpeedBytesPerSec, ETASeconds and
// SuggestedFilename are runtime-only and are never written.
type DownloadRepository struct {
	db *sql.DB
}

// NewDownloadRepository returns a repository backed by db. The caller
// keeps ownership of db (open it with Open, close it when done).
func NewDownloadRepository(db *sql.DB) *DownloadRepository {
	return &DownloadRepository{db: db}
}

// Close closes the underlying database connection. The repository
// shares the *sql.DB with the settings repository (same handle opened
// by Open), so Close must only be called at shutdown, when no further
// settings or download writes will happen.
func (r *DownloadRepository) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

var _ domain.DownloadRepository = (*DownloadRepository)(nil)

// downloadColumns is the persisted-field column list, in scan order.
// queue_position is excluded: it is managed by Insert/Move, never by
// the entity round-trip.
const downloadColumns = `id, url, filename, destination_dir, temp_path, final_path, ` +
	`status, total_bytes, downloaded_bytes, etag, last_modified, supports_range, ` +
	`error_code, error_message, created_at, updated_at`

// rowScanner is satisfied by *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

// scanDownload reads one download row in downloadColumns order.
func scanDownload(row rowScanner) (*domain.Download, error) {
	var d domain.Download
	var status string
	var total sql.NullInt64
	var etag, lastModified sql.NullString
	var supportsRange int
	var errorCode string
	var createdAt, updatedAt string
	if err := row.Scan(
		&d.ID, &d.URL, &d.Filename, &d.DestinationDir, &d.TempPath, &d.FinalPath,
		&status, &total, &d.DownloadedBytes, &etag, &lastModified, &supportsRange,
		&errorCode, &d.ErrorMessage, &createdAt, &updatedAt,
	); err != nil {
		return nil, err
	}
	d.Status = domain.Status(status)
	if total.Valid {
		d.TotalBytes = total.Int64
	} else {
		d.TotalBytes = -1
	}
	if etag.Valid {
		d.ETag = etag.String
	}
	if lastModified.Valid {
		d.LastModified = lastModified.String
	}
	d.SupportsRange = supportsRange != 0
	d.ErrorCode = domain.ErrorCode(errorCode)
	var err error
	if d.CreatedAt, err = time.Parse(time.RFC3339, createdAt); err != nil {
		return nil, fmt.Errorf("sqlite: parse created_at %q: %w", createdAt, err)
	}
	if d.UpdatedAt, err = time.Parse(time.RFC3339, updatedAt); err != nil {
		return nil, fmt.Errorf("sqlite: parse updated_at %q: %w", updatedAt, err)
	}
	return &d, nil
}

// nullIfEmpty stores "" as NULL so an unset validator round-trips as
// empty rather than as a stored empty string.
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// nullTotalBytes stores -1 (unknown size) as NULL.
func nullTotalBytes(v int64) any {
	if v < 0 {
		return nil
	}
	return v
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// dbOrErr returns the backing DB or an error when the repository was
// built with a nil DB.
func (r *DownloadRepository) dbOrErr() (*sql.DB, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("sqlite: DownloadRepository used with nil DB")
	}
	return r.db, nil
}

// notFound wraps domain.ErrDownloadNotFound with the addressed ID.
func notFound(id string) error {
	return fmt.Errorf("sqlite: download %q: %w", id, domain.ErrDownloadNotFound)
}

// Insert stores a new download, appending it at the end of the queue
// (queue_position = max + 1, or 0 when the table is empty).
func (r *DownloadRepository) Insert(ctx context.Context, d *domain.Download) error {
	db, err := r.dbOrErr()
	if err != nil {
		return err
	}
	if d == nil {
		return fmt.Errorf("sqlite: Insert called with nil Download")
	}
	if d.ID == "" {
		return fmt.Errorf("sqlite: Insert called with empty download ID")
	}
	now := time.Now().UTC()
	if d.CreatedAt.IsZero() {
		d.CreatedAt = now
	}
	if d.UpdatedAt.IsZero() {
		d.UpdatedAt = now
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: Insert %q: begin: %w", d.ID, err)
	}
	defer func() { _ = tx.Rollback() }()
	var max sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(queue_position) FROM downloads`).Scan(&max); err != nil {
		return fmt.Errorf("sqlite: Insert %q: read max queue_position: %w", d.ID, err)
	}
	position := int64(0)
	if max.Valid {
		position = max.Int64 + 1
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO downloads (`+downloadColumns+`, queue_position) `+
			`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		d.ID, d.URL, d.Filename, d.DestinationDir, d.TempPath, d.FinalPath,
		string(d.Status), nullTotalBytes(d.TotalBytes), d.DownloadedBytes,
		nullIfEmpty(d.ETag), nullIfEmpty(d.LastModified), boolToInt(d.SupportsRange),
		string(d.ErrorCode), d.ErrorMessage,
		d.CreatedAt.UTC().Format(time.RFC3339), d.UpdatedAt.UTC().Format(time.RFC3339),
		position,
	); err != nil {
		return fmt.Errorf("sqlite: Insert %q: %w", d.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: Insert %q: commit: %w", d.ID, err)
	}
	return nil
}

// GetByID returns the download with the given ID.
func (r *DownloadRepository) GetByID(ctx context.Context, id string) (*domain.Download, error) {
	db, err := r.dbOrErr()
	if err != nil {
		return nil, err
	}
	d, err := scanDownload(db.QueryRowContext(ctx,
		`SELECT `+downloadColumns+` FROM downloads WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, notFound(id)
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite: GetByID %q: %w", id, err)
	}
	return d, nil
}

// ListAll returns every download in queue order (queue_position ASC;
// id ASC breaks ties deterministically).
func (r *DownloadRepository) ListAll(ctx context.Context) ([]*domain.Download, error) {
	db, err := r.dbOrErr()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx,
		`SELECT `+downloadColumns+` FROM downloads ORDER BY queue_position ASC, id ASC`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: ListAll: %w", err)
	}
	defer rows.Close()
	out := []*domain.Download{}
	for rows.Next() {
		d, err := scanDownload(rows)
		if err != nil {
			return nil, fmt.Errorf("sqlite: ListAll: scan: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: ListAll: %w", err)
	}
	return out, nil
}

// Update rewrites every persisted field of d except queue_position.
func (r *DownloadRepository) Update(ctx context.Context, d *domain.Download) error {
	db, err := r.dbOrErr()
	if err != nil {
		return err
	}
	if d == nil {
		return fmt.Errorf("sqlite: Update called with nil Download")
	}
	if d.ID == "" {
		return fmt.Errorf("sqlite: Update called with empty download ID")
	}
	if d.UpdatedAt.IsZero() {
		d.UpdatedAt = time.Now().UTC()
	}
	res, err := db.ExecContext(ctx,
		`UPDATE downloads SET url = ?, filename = ?, destination_dir = ?, `+
			`temp_path = ?, final_path = ?, status = ?, total_bytes = ?, `+
			`downloaded_bytes = ?, etag = ?, last_modified = ?, supports_range = ?, `+
			`error_code = ?, error_message = ?, created_at = ?, updated_at = ? `+
			`WHERE id = ?`,
		d.URL, d.Filename, d.DestinationDir, d.TempPath, d.FinalPath,
		string(d.Status), nullTotalBytes(d.TotalBytes), d.DownloadedBytes,
		nullIfEmpty(d.ETag), nullIfEmpty(d.LastModified), boolToInt(d.SupportsRange),
		string(d.ErrorCode), d.ErrorMessage,
		d.CreatedAt.UTC().Format(time.RFC3339), d.UpdatedAt.UTC().Format(time.RFC3339),
		d.ID,
	)
	if err != nil {
		return fmt.Errorf("sqlite: Update %q: %w", d.ID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: Update %q: rows affected: %w", d.ID, err)
	}
	if n == 0 {
		return notFound(d.ID)
	}
	return nil
}

// UpdateStatus changes only the status (and updated_at) of one download.
func (r *DownloadRepository) UpdateStatus(ctx context.Context, id string, status domain.Status, updatedAt time.Time) error {
	db, err := r.dbOrErr()
	if err != nil {
		return err
	}
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	res, err := db.ExecContext(ctx,
		`UPDATE downloads SET status = ?, updated_at = ? WHERE id = ?`,
		string(status), updatedAt.UTC().Format(time.RFC3339), id)
	if err != nil {
		return fmt.Errorf("sqlite: UpdateStatus %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: UpdateStatus %q: rows affected: %w", id, err)
	}
	if n == 0 {
		return notFound(id)
	}
	return nil
}

// Delete removes the download row; its segment rows go with it via the
// foreign-key cascade (foreign_keys is ON for the connection).
func (r *DownloadRepository) Delete(ctx context.Context, id string) error {
	db, err := r.dbOrErr()
	if err != nil {
		return err
	}
	res, err := db.ExecContext(ctx, `DELETE FROM downloads WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("sqlite: Delete %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: Delete %q: rows affected: %w", id, err)
	}
	if n == 0 {
		return notFound(id)
	}
	return nil
}

// Move places the download at the given 0-based queue position,
// shifting the others. Positions outside [0, n-1] clamp to the nearest
// end; the rewrite runs in one transaction, so the order is never left
// half-updated. Positions are renumbered densely 0..n-1, which also
// closes any gap left by a Delete.
func (r *DownloadRepository) Move(ctx context.Context, id string, position int) error {
	db, err := r.dbOrErr()
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: Move %q: begin: %w", id, err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `SELECT id FROM downloads ORDER BY queue_position ASC, id ASC`)
	if err != nil {
		return fmt.Errorf("sqlite: Move %q: list queue: %w", id, err)
	}
	var ids []string
	for rows.Next() {
		var cur string
		if err := rows.Scan(&cur); err != nil {
			_ = rows.Close()
			return fmt.Errorf("sqlite: Move %q: scan queue: %w", id, err)
		}
		ids = append(ids, cur)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("sqlite: Move %q: list queue: %w", id, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("sqlite: Move %q: list queue: %w", id, err)
	}
	from := -1
	for i, cur := range ids {
		if cur == id {
			from = i
			break
		}
	}
	if from < 0 {
		return notFound(id)
	}
	if position < 0 {
		position = 0
	}
	if position > len(ids)-1 {
		position = len(ids) - 1
	}
	if position == from {
		// No-op: nothing to rewrite, just commit the (read-only)
		// transaction and return.
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("sqlite: Move %q: commit: %w", id, err)
		}
		return nil
	}
	ids = append(ids[:from], ids[from+1:]...)
	ids = append(ids, "")
	copy(ids[position+1:], ids[position:])
	ids[position] = id
	for i, cur := range ids {
		if _, err := tx.ExecContext(ctx,
			`UPDATE downloads SET queue_position = ? WHERE id = ?`, i, cur); err != nil {
			return fmt.Errorf("sqlite: Move %q: reorder: %w", id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: Move %q: commit: %w", id, err)
	}
	return nil
}
