package schemamigrator

import "testing"

func TestLogsMigrations(t *testing.T) {
	manager := newTestMigrationManager(t)
	checkSchemaMigratorMigrationRecords(t, manager, LogsMigrations)
}

func TestLogsMigrationsExactNature(t *testing.T) {
	manager := newTestMigrationManager(t)
	checkSchemaMigrationRecordExactNature(
		t,
		manager,
		[]SchemaMigrationRecord{
			LogsMigrations[1],
			LogsMigrations[2],
			LogsMigrations[4],
			LogsMigrations[5],
			LogsMigrations[6], // 2001 (sync)
			LogsMigrations[9], // 2004 (sync)
			LogsMigrations[10], // 2005 (sync): tenant-scoped log metadata tables
			LogsMigrations[11], // 2006 (sync): tenant-scoped log tag values
		},
		[]SchemaMigrationRecord{
			LogsMigrations[0], // 1000 (async)
			LogsMigrations[3], // 1003 (async)
		},
	)
}
