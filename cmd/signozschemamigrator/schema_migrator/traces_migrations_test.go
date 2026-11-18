package schemamigrator

import (
	"testing"
)

func TestTracesMigrations(t *testing.T) {
	manager := newTestMigrationManager(t)
	checkSchemaMigratorMigrationRecords(t, manager, TracesMigrations)
}

func TestTracesMigrationsExactNature(t *testing.T) {
	manager := newTestMigrationManager(t)
	checkSchemaMigrationRecordExactNature(
		t,
		manager,
		[]SchemaMigrationRecord{
			TracesMigrations[0],
			TracesMigrations[2],
			TracesMigrations[4],
			TracesMigrations[5],
			TracesMigrations[6],
			TracesMigrations[7],
			TracesMigrations[8],
			TracesMigrations[11],
			TracesMigrations[12],
			TracesMigrations[13],
			TracesMigrations[14],
			TracesMigrations[15],
			TracesMigrations[16],
			TracesMigrations[17],
			TracesMigrations[18],
			TracesMigrations[19], // 1019 (tenant metadata columns)
			TracesMigrations[20], // 1020 (tenant-keyed metadata tables)
		},
		[]SchemaMigrationRecord{
			TracesMigrations[1], // 1001 (async)
			TracesMigrations[3], // 1003 (async)
		},
	)
}
