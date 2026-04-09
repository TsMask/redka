package store

import "fmt"

// Migrate runs database migrations using GORM AutoMigrate.
// Creates/updates tables from GORM model definitions.
// Version tracking, mtime updates, and cardinality (len) tracking
// are handled in Go application code, not via database triggers.
func (s *Store) Migrate() error {
	if err := s.autoMigrate(); err != nil {
		return fmt.Errorf("auto migrate: %w", err)
	}
	return nil
}

// autoMigrate creates/updates tables using GORM AutoMigrate.
func (s *Store) autoMigrate() error {
	return s.DB.AutoMigrate(
		&RKey{},
		&RString{},
		&RHash{},
		&RList{},
		&RSet{},
		&RZSet{},
	)
}
