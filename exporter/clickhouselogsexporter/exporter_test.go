package clickhouselogsexporter

import (
	"context"
	"log"
	"math"
	"strings"
	"testing"
	"time"

	driver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz-otel-collector/constants"
	"github.com/SigNoz/signoz-otel-collector/pkg/pdatagen/plogsgen"
	"github.com/SigNoz/signoz-otel-collector/pkg/tagdedup"
	"github.com/SigNoz/signoz-otel-collector/usage"
	"github.com/SigNoz/signoz-otel-collector/utils"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/jellydator/ttlcache/v3"
	cmock "github.com/srikanthccv/ClickHouse-go-mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"

	"github.com/SigNoz/signoz-otel-collector/exporter/clickhouselogsexporter/internal/metadata"
)

func eventually(t *testing.T, f func() bool) {
	assert.Eventually(t, f, 10*time.Second, 100*time.Millisecond)
}

func testOptions(t *testing.T) []LogExporterOption {
	// keys cache is used to avoid duplicate inserts for the same attribute key.
	keysCache := ttlcache.New(
		ttlcache.WithTTL[string, struct{}](240*time.Minute),
		ttlcache.WithCapacity[string, struct{}](50000),
		ttlcache.WithDisableTouchOnHit[string, struct{}](),
	)
	go keysCache.Start()

	// resource fingerprint cache is used to avoid duplicate inserts for the same resource fingerprint.
	// the ttl is set to the same as the bucket rounded value i.e 1800 seconds.
	// if a resource fingerprint is seen in the bucket already, skip inserting it again.
	rfCache := ttlcache.New(
		ttlcache.WithTTL[string, struct{}](distributedLogsResourceV2Seconds*time.Second),
		ttlcache.WithDisableTouchOnHit[string, struct{}](),
		ttlcache.WithCapacity[string, struct{}](100000),
	)
	go rfCache.Start()

	t.Cleanup(func() {
		keysCache.Stop()
		rfCache.Stop()
	})

	return []LogExporterOption{
		WithLogger(zap.NewNop()),
		WithMeter(noop.NewMeterProvider().Meter(metadata.ScopeName)),
		WithKeysCache(keysCache),
		WithRFCache(rfCache),
	}
}

// setupTestExporter creates a new exporter with mock ClickHouse client for testing
func setupTestExporter(t *testing.T, mock driver.Conn) *clickhouseLogsExporter {
	opts := testOptions(t)
	opts = append(opts, WithClickHouseClient(mock))
	id := uuid.New()
	opts = append(opts, WithNewUsageCollector(id, mock))

	cfg := &Config{
		DSN: "clickhouse://localhost:9000/test",
		AttributesLimits: AttributesLimits{
			FetchKeysInterval: 2 * time.Second,
			MaxDistinctValues: 25000,
		},
	}

	require.NoError(t, cfg.Validate())

	exporter, err := newExporter(
		exporter.Settings{},
		cfg,
		opts...,
	)

	if err != nil {
		t.Fatalf("failed to create exporter: %v", err)
	}

	return exporter
}

func TestExporterInit(t *testing.T) {
	mock, err := cmock.NewClickHouseWithQueryMatcher(nil, sqlmock.QueryMatcherRegexp)
	if err != nil {
		log.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	cols := []cmock.ColumnType{
		{Name: "org_id", Type: "String"},
		{Name: "project_id", Type: "String"},
		{Name: "tag_key", Type: "String"},
		{Name: "tag_type", Type: "String"},
		{Name: "tag_data_type", Type: "String"},
		{Name: "string_count", Type: "UInt64"},
		{Name: "number_count", Type: "UInt64"},
	}
	legacyRows := cmock.NewRows(cols, [][]interface{}{
		{"", "", "key1", "string", "string", 2, 1},
		{"", "", "key2", "number", "number", 1, 2},
	})
	tenantRows := cmock.NewRows(cols, [][]interface{}{
		{"11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", "key1", "string", "string", 2, 1},
	})

	mock.ExpectSelect(".*SETTINGS max_threads = 2").WillReturnRows(legacyRows)
	mock.ExpectSelect(".*SETTINGS max_threads = 2").WillReturnRows(tenantRows)
	mock.ExpectClose()

	exporter := setupTestExporter(t, mock)
	err = exporter.Start(context.Background(), nil)
	assert.Nil(t, err)

	err = exporter.Shutdown(context.Background())
	assert.Nil(t, err)

	eventually(t, func() bool {
		return mock.ExpectationsWereMet() == nil
	})
}

func TestExporterPushLogsData(t *testing.T) {
	mock, err := cmock.NewClickHouseWithQueryMatcher(nil, sqlmock.QueryMatcherRegexp)
	if err != nil {
		log.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	// expect prepare batch for 5 tables
	tagStatementV2 := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_tag_attributes_v2")
	attributeKeysStmt := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_logs_attribute_keys")
	resourceKeysStmt := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_logs_resource_keys")
	logsStatementV2 := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_logs_v2.*")
	logsResourceStatementV2 := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_logs_v2_resource.*")

	tagStatementV2.ExpectAppend()
	tagStatementV2.ExpectSend()
	attributeKeysStmt.ExpectAppend()
	attributeKeysStmt.ExpectSend()
	resourceKeysStmt.ExpectAppend()
	resourceKeysStmt.ExpectSend()
	logsStatementV2.ExpectAppend()
	logsStatementV2.ExpectSend()
	logsResourceStatementV2.ExpectAppend()
	logsResourceStatementV2.ExpectSend()

	// make sure usage is inserted on shutdown
	mock.ExpectExec(".*insert into signoz_logs.distributed_usage.*").WithArgs()
	mock.ExpectClose()

	exporter := setupTestExporter(t, mock)

	logs := plogsgen.Generate()
	large := strings.Repeat("x", 10000)
	resourceLogs := logs.ResourceLogs().At(0)
	resourceLogs.SetSchemaUrl(large)
	resourceLogs.Resource().SetDroppedAttributesCount(7)
	scopeLogs := resourceLogs.ScopeLogs().At(0)
	scopeLogs.SetSchemaUrl(large)
	scopeLogs.Scope().SetName(large)
	scopeLogs.Scope().SetVersion(large)
	scopeLogs.Scope().Attributes().PutStr("scope.attribute", large)
	scopeLogs.Scope().SetDroppedAttributesCount(7)
	record := scopeLogs.LogRecords().At(0)
	record.SetSeverityText(large)
	record.SetSeverityNumber(plog.SeverityNumberFatal4)
	record.SetEventName(large)
	record.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	record.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
	record.SetFlags(plog.DefaultLogRecordFlags.WithIsSampled(true))
	record.SetDroppedAttributesCount(7)

	err = exporter.pushLogsData(context.Background(), logs)
	if err != nil {
		t.Fatalf("failed to push logs data: %v", err)
	}

	eventually(t, func() bool {
		rows, err := view.RetrieveData(SigNozLogsCount)
		return err == nil && len(rows) > 0
	})

	// Only body "This is a test log record" (25) + attributes {} (2) + resource {"resource.0":"resource"} (25) count.
	var size float64
	rows, err := view.RetrieveData(SigNozLogsBytes)
	require.NoError(t, err)
	for _, row := range rows {
		for _, tg := range row.Tags {
			if tg.Key == usage.TagExporterIdKey && tg.Value == exporter.id.String() {
				size += row.Data.(*view.SumData).Value
			}
		}
	}
	assert.Equal(t, float64(25+2+25), size)

	err = exporter.Shutdown(context.Background())
	assert.Nil(t, err)

	eventually(t, func() bool {
		return mock.ExpectationsWereMet() == nil
	})
}

func TestExporterPushLogsDataEventNameNotBilled(t *testing.T) {
	testCases := []struct {
		name      string
		eventName string
	}{
		{name: "EventNameEmpty_BilledBodyAttrsResource", eventName: ""},
		{name: "EventNameSet_SameBilledSize", eventName: strings.Repeat("signoz.deployment", 100)},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			mock, err := cmock.NewClickHouseWithQueryMatcher(nil, sqlmock.QueryMatcherRegexp)
			require.NoError(t, err)

			tagStatementV2 := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_tag_attributes_v2")
			attributeKeysStmt := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_logs_attribute_keys")
			resourceKeysStmt := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_logs_resource_keys")
			logsStatementV2 := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_logs_v2.*")
			logsResourceStatementV2 := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_logs_v2_resource.*")
			tagStatementV2.ExpectAppend()
			tagStatementV2.ExpectSend()
			attributeKeysStmt.ExpectAppend()
			attributeKeysStmt.ExpectSend()
			resourceKeysStmt.ExpectAppend()
			resourceKeysStmt.ExpectSend()
			logsStatementV2.ExpectAppend()
			logsStatementV2.ExpectSend()
			logsResourceStatementV2.ExpectAppend()
			logsResourceStatementV2.ExpectSend()
			mock.ExpectExec(".*insert into signoz_logs.distributed_usage.*").WithArgs()
			mock.ExpectClose()

			exporter := setupTestExporter(t, mock)
			logs := plogsgen.Generate()
			logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SetEventName(testCase.eventName)

			require.NoError(t, exporter.pushLogsData(context.Background(), logs))

			billedBytes := func() float64 {
				var size float64
				rows, err := view.RetrieveData(SigNozLogsBytes)
				if err != nil {
					return 0
				}
				for _, row := range rows {
					for _, tg := range row.Tags {
						if tg.Key == usage.TagExporterIdKey && tg.Value == exporter.id.String() {
							size += row.Data.(*view.SumData).Value
						}
					}
				}
				return size
			}
			eventually(t, func() bool { return billedBytes() > 0 })

			// Only body "This is a test log record" (25) + attributes {} (2) + resource {"resource.0":"resource"} (25) count.
			assert.Equal(t, float64(25+2+25), billedBytes())

			assert.NoError(t, exporter.Shutdown(context.Background()))
			eventually(t, func() bool { return mock.ExpectationsWereMet() == nil })
		})
	}
}

func TestAttributesToMapSkipsOriginalBodyAttribute(t *testing.T) {
	attrs := pcommon.NewMap()
	attrs.PutStr("regular", "value")
	attrs.PutStr(constants.OriginalBodyAttributeKey, "raw body")
	attrs.PutInt("num", 1)

	for _, forceStringValues := range []bool{true, false} {
		m := attributesToMap(attrs, forceStringValues)
		_, exists := m.StringData[constants.OriginalBodyAttributeKey]
		require.False(t, exists)
		require.Equal(t, "value", m.StringData["regular"])
	}
}

func TestGetResourceAttributesByte(t *testing.T) {
	tests := []struct {
		name      string
		attribute string
		pass      bool
	}{
		{
			name:      "add_signoz_resource_attribute",
			attribute: "signoz.workspace.internal.test",
			pass:      true,
		},
		{
			name:      "add_non_signoz_resource_attribute",
			attribute: "nonsignoz.internal.test",
			pass:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plogs := plogsgen.Generate(plogsgen.WithResourceAttributeCount(10))

			// get the base expected value
			withoutSigNozAttrs, err := getResourceAttributesByte(plogs.ResourceLogs().At(0).Resource())
			require.NoError(t, err)

			// add the provided attributeand get the actual value
			plogs.ResourceLogs().At(0).Resource().Attributes().PutStr(tc.attribute, "test")
			WithNewAttribute, err := getResourceAttributesByte(plogs.ResourceLogs().At(0).Resource())
			require.NoError(t, err)

			if tc.pass {
				assert.Equal(t, withoutSigNozAttrs, WithNewAttribute)
			} else {
				assert.NotEqual(t, withoutSigNozAttrs, WithNewAttribute)
			}

		})
	}
}

// setupTestExporterWithConcurrency creates a new exporter with mock ClickHouse client and custom concurrency for testing
func setupTestExporterWithConcurrency(t *testing.T, mock driver.Conn, concurrency int) *clickhouseLogsExporter {
	opts := testOptions(t)
	id := uuid.New()
	opts = append(opts, WithClickHouseClient(mock), WithNewUsageCollector(id, mock), WithConcurrency(concurrency))

	cfg := &Config{
		DSN: "clickhouse://localhost:9000/test",
		AttributesLimits: AttributesLimits{
			FetchKeysInterval: 2 * time.Second,
			MaxDistinctValues: 25000,
		},
	}

	require.NoError(t, cfg.Validate())

	exporter, err := newExporter(
		exporter.Settings{},
		cfg,
		opts...,
	)

	if err != nil {
		t.Fatalf("failed to create exporter: %v", err)
	}

	return exporter
}

func TestConcurrencyCapacity(t *testing.T) {
	exporter := setupTestExporterWithConcurrency(t, nil, 7)
	assert.Equal(t, 7, cap(exporter.limiter))

	eventually(t, func() bool {
		return exporter.Shutdown(context.Background()) == nil
	})
}

func TestExporterConcurrency(t *testing.T) {
	tests := []struct {
		name        string
		logCount    int
		concurrency int
	}{
		{
			name:        "1_log",
			logCount:    1,
			concurrency: 3,
		},
		{
			name:        "7_logs",
			logCount:    7,
			concurrency: 3,
		},
		{
			name:        "10k_logs",
			logCount:    10000,
			concurrency: 3,
		},
		{
			name:        "2234_logs",
			logCount:    2234,
			concurrency: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock, err := cmock.NewClickHouseWithQueryMatcher(nil, sqlmock.QueryMatcherRegexp)
			if err != nil {
				log.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
			}

			// expect prepare batch for 5 tables
			tagStatementV2 := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_tag_attributes_v2")
			attributeKeysStmt := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_logs_attribute_keys")
			resourceKeysStmt := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_logs_resource_keys")
			logsStatementV2 := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_logs_v2.*")
			logsResourceStatementV2 := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_logs_v2_resource.*")

			tagStatementV2.ExpectAppend()
			tagStatementV2.ExpectSend()
			attributeKeysStmt.ExpectAppend()
			attributeKeysStmt.ExpectSend()
			resourceKeysStmt.ExpectAppend()
			resourceKeysStmt.ExpectSend()
			// Expect appends for logs matching the log count
			for i := 0; i < tc.logCount; i++ {
				logsStatementV2.ExpectAppend()
			}
			logsStatementV2.ExpectSend()
			logsResourceStatementV2.ExpectAppend()
			logsResourceStatementV2.ExpectSend()

			mock.ExpectClose()

			exporter := setupTestExporterWithConcurrency(t, mock, tc.concurrency)
			logs := plogsgen.Generate(plogsgen.WithLogRecordCount(tc.logCount))

			err = exporter.pushLogsData(context.Background(), logs)
			require.NoError(t, err)

			err = exporter.Shutdown(context.Background())
			assert.Nil(t, err)

			eventually(t, func() bool {
				return mock.ExpectationsWereMet() == nil
			})
		})
	}
}

func TestProcessBody(t *testing.T) {
	testCases := []struct {
		name             string
		bodyJSONEnabled  bool
		promotedPaths    map[string]struct{}
		body             func() pcommon.Value
		originalBody     func() pcommon.Value
		expectedBody     string
		expectedBodyJSON string
		expectedPromoted string
	}{
		{
			name:            "BodyJSONDisabled_StringBody",
			bodyJSONEnabled: false,
			promotedPaths:   map[string]struct{}{},
			body: func() pcommon.Value {
				return pcommon.NewValueStr("test log message")
			},
			expectedBody:     "test log message",
			expectedBodyJSON: "{}",
			expectedPromoted: "{}",
		},
		{
			name:            "BodyJSONDisabled_MapBody",
			bodyJSONEnabled: false,
			promotedPaths:   map[string]struct{}{"message": {}},
			body: func() pcommon.Value {
				v := pcommon.NewValueMap()
				v.Map().PutStr("message", "structured")
				return v
			},
			expectedBody:     `{"message":"structured"}`,
			expectedBodyJSON: "{}",
			expectedPromoted: "{}",
		},
		{
			name:            "BodyJSONEnabled_Stash_Ignored",
			bodyJSONEnabled: true,
			promotedPaths:   map[string]struct{}{},
			body: func() pcommon.Value {
				v := pcommon.NewValueMap()
				v.Map().PutStr("message", "raw text log")
				return v
			},
			originalBody: func() pcommon.Value {
				return pcommon.NewValueStr("spoofed original body")
			},
			expectedBody:     "",
			expectedBodyJSON: `{"message":"raw text log"}`,
			expectedPromoted: "{}",
		},
		{
			name:            "BodyJSONEnabled_StringBody_NoStash_BodyEmpty",
			bodyJSONEnabled: true,
			promotedPaths:   map[string]struct{}{"message": {}},
			body: func() pcommon.Value {
				return pcommon.NewValueStr("plain line")
			},
			expectedBody:     "",
			expectedBodyJSON: `{"message":"plain line"}`,
			expectedPromoted: `{"message":"plain line"}`,
		},
		{
			name:            "BodyJSONEnabled_IntBody_Wrapped",
			bodyJSONEnabled: true,
			promotedPaths:   map[string]struct{}{"message": {}},
			body: func() pcommon.Value {
				return pcommon.NewValueInt(42)
			},
			expectedBody:     "",
			expectedBodyJSON: `{"message":"42"}`,
			expectedPromoted: `{"message":"42"}`,
		},
		{
			name:            "BodyJSONEnabled_SliceBody_Wrapped",
			bodyJSONEnabled: true,
			promotedPaths:   map[string]struct{}{},
			body: func() pcommon.Value {
				v := pcommon.NewValueSlice()
				v.Slice().AppendEmpty().SetStr("a")
				v.Slice().AppendEmpty().SetInt(1)
				return v
			},
			expectedBody:     "",
			expectedBodyJSON: `{"message":"[\"a\",1]"}`,
			expectedPromoted: "{}",
		},
		{
			name:            "BodyJSONEnabled_BytesBody_Wrapped",
			bodyJSONEnabled: true,
			promotedPaths:   map[string]struct{}{},
			body: func() pcommon.Value {
				v := pcommon.NewValueBytes()
				v.Bytes().Append([]byte("raw bytes")...)
				return v
			},
			expectedBody:     "",
			expectedBodyJSON: `{"message":"raw bytes"}`,
			expectedPromoted: "{}",
		},
		{
			name:            "BodyJSONEnabled_EmptyBody_Wrapped",
			bodyJSONEnabled: true,
			promotedPaths:   map[string]struct{}{},
			body: func() pcommon.Value {
				return pcommon.NewValueEmpty()
			},
			expectedBody:     "",
			expectedBodyJSON: `{"message":""}`,
			expectedPromoted: "{}",
		},
		{
			name:            "BodyJSONEnabled_MapBody_NoStash_BodyEmpty",
			bodyJSONEnabled: true,
			promotedPaths:   map[string]struct{}{"message": {}},
			body: func() pcommon.Value {
				v := pcommon.NewValueMap()
				v.Map().PutStr("message", "test")
				return v
			},
			expectedBody:     "",
			expectedBodyJSON: `{"message":"test"}`,
			expectedPromoted: `{"message":"test"}`,
		},
		{
			name:            "BodyJSONEnabled_MapBody_NonFiniteAndOutOfInt64Doubles_Sanitized",
			bodyJSONEnabled: true,
			promotedPaths:   map[string]struct{}{"big": {}},
			body: func() pcommon.Value {
				v := pcommon.NewValueMap()
				v.Map().PutStr("message", "m")
				v.Map().PutDouble("nan", math.NaN())
				v.Map().PutDouble("big", 18446744073709551615)
				v.Map().PutDouble("neg", -1e19)
				v.Map().PutDouble("ok", 1.5)
				s := v.Map().PutEmptySlice("arr")
				s.AppendEmpty().SetDouble(math.Inf(1))
				s.AppendEmpty().SetEmptyMap().PutDouble("h", 1e20)
				return v
			},
			expectedBody:     "",
			expectedBodyJSON: `{"arr":[null,{"h":1e+20}],"big":1.8446744073709552e+19,"message":"m","nan":null,"neg":-1e+19,"ok":1.5}`,
			expectedPromoted: `{"big":1.8446744073709552e+19}`,
		},
		{
			name:            "BodyJSONEnabled_MapBody_Int64RangeValues_Unchanged",
			bodyJSONEnabled: true,
			promotedPaths:   map[string]struct{}{},
			body: func() pcommon.Value {
				v := pcommon.NewValueMap()
				v.Map().PutInt("max", math.MaxInt64)
				v.Map().PutInt("min", math.MinInt64)
				v.Map().PutDouble("whole", 200)
				v.Map().PutDouble("tiny", 1e-7)
				return v
			},
			expectedBody:     "",
			expectedBodyJSON: `{"max":9223372036854775807,"min":-9223372036854775808,"tiny":1e-07,"whole":200}`,
			expectedPromoted: "{}",
		},
		{
			name:            "BodyJSONDisabled_MapBody_Stash_PromotedPaths",
			bodyJSONEnabled: false,
			promotedPaths:   map[string]struct{}{"message": {}},
			body: func() pcommon.Value {
				v := pcommon.NewValueMap()
				v.Map().PutStr("message", "test")
				v.Map().PutInt("level", 1)
				return v
			},
			originalBody: func() pcommon.Value {
				return pcommon.NewValueStr(`{"level":1,"message":"test"}`)
			},
			expectedBody:     `{"level":1,"message":"test"}`,
			expectedBodyJSON: `{"level":1,"message":"test"}`,
			expectedPromoted: `{"message":"test"}`,
		},
		{
			name:            "BodyJSONDisabled_MapBody_Stash_NestedPromotedPaths",
			bodyJSONEnabled: false,
			promotedPaths: map[string]struct{}{
				"user.id": {},
				"message": {},
			},
			body: func() pcommon.Value {
				v := pcommon.NewValueMap()
				v.Map().PutStr("message", "test")
				userMap := v.Map().PutEmptyMap("user")
				userMap.PutStr("id", "123")
				userMap.PutStr("name", "john")
				return v
			},
			originalBody: func() pcommon.Value {
				return pcommon.NewValueStr(`{"message":"test","user":{"id":"123","name":"john"}}`)
			},
			expectedBody:     `{"message":"test","user":{"id":"123","name":"john"}}`,
			expectedBodyJSON: `{"message":"test","user":{"id":"123","name":"john"}}`,
			expectedPromoted: `{"message":"test","user.id":"123"}`,
		},
		{
			name:            "BodyJSONDisabled_MapBody_Stash_MultiplePromotedPaths",
			bodyJSONEnabled: false,
			promotedPaths: map[string]struct{}{
				"level":      {},
				"user.id":    {},
				"user.name":  {},
				"user.roles": {},
				"message":    {},
			},
			body: func() pcommon.Value {
				v := pcommon.NewValueMap()
				v.Map().PutStr("message", "test")
				v.Map().PutInt("level", 1)
				userMap := v.Map().PutEmptyMap("user")
				userMap.PutStr("id", "123")
				userMap.PutStr("name", "john")
				userMap.PutStr("email", "john@example.com")
				roles := userMap.PutEmptySlice("roles")
				roles.AppendEmpty().SetStr("admin")
				roles.AppendEmpty().SetStr("user")
				return v
			},
			originalBody: func() pcommon.Value {
				return pcommon.NewValueStr(`{"level":1,"message":"test","user":{"email":"john@example.com","id":"123","name":"john","roles":["admin","user"]}}`)
			},
			expectedBody:     `{"level":1,"message":"test","user":{"email":"john@example.com","id":"123","name":"john","roles":["admin","user"]}}`,
			expectedBodyJSON: `{"level":1,"message":"test","user":{"email":"john@example.com","id":"123","name":"john","roles":["admin","user"]}}`,
			expectedPromoted: `{"level":1,"message":"test","user.id":"123","user.name":"john","user.roles":["admin","user"]}`,
		},
		{
			name:            "BodyJSONDisabled_MapBody_Stash_RawTextRestored",
			bodyJSONEnabled: false,
			promotedPaths:   map[string]struct{}{},
			body: func() pcommon.Value {
				v := pcommon.NewValueMap()
				v.Map().PutStr("message", "raw text log")
				return v
			},
			originalBody: func() pcommon.Value {
				return pcommon.NewValueStr("raw text log")
			},
			expectedBody:     "raw text log",
			expectedBodyJSON: `{"message":"raw text log"}`,
			expectedPromoted: "{}",
		},
		{
			name:            "BodyJSONDisabled_MapBody_Stash_NormalizedJSONStringRestored",
			bodyJSONEnabled: false,
			promotedPaths:   map[string]struct{}{},
			body: func() pcommon.Value {
				v := pcommon.NewValueMap()
				v.Map().PutStr("message", "hi")
				v.Map().PutStr("level", "info")
				return v
			},
			originalBody: func() pcommon.Value {
				return pcommon.NewValueStr(`{"msg": "hi",   "level": "info"}`)
			},
			expectedBody:     `{"msg": "hi",   "level": "info"}`,
			expectedBodyJSON: `{"level":"info","message":"hi"}`,
			expectedPromoted: "{}",
		},
		{
			name:            "BodyJSONDisabled_StringBody_Stash_Restored",
			bodyJSONEnabled: false,
			promotedPaths:   map[string]struct{}{},
			body: func() pcommon.Value {
				return pcommon.NewValueStr("stringified by a pipeline")
			},
			originalBody: func() pcommon.Value {
				return pcommon.NewValueStr("original line")
			},
			expectedBody:     "original line",
			expectedBodyJSON: `{"message":"stringified by a pipeline"}`,
			expectedPromoted: "{}",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Create exporter with test configuration
			opts := testOptions(t)
			opts = append(opts, WithClickHouseClient(nil))
			id := uuid.New()
			opts = append(opts, WithNewUsageCollector(id, nil))

			exporter, err := newExporter(
				exporter.Settings{},
				&Config{
					DSN:                       "clickhouse://localhost:9000/test",
					BodyJSONEnabled:           testCase.bodyJSONEnabled,
					PromotedPathsSyncInterval: utils.ToPointer(5 * time.Minute),
					LogLevelConcurrency:       utils.ToPointer(1),
					AttributesLimits: AttributesLimits{
						FetchKeysInterval: 2 * time.Second,
						MaxDistinctValues: 25000,
					},
				},
				opts...,
			)
			require.NoError(t, err)

			// Set promoted paths
			if testCase.promotedPaths != nil {
				exporter.promotedPaths.Store(testCase.promotedPaths)
			} else {
				exporter.promotedPaths.Store(map[string]struct{}{})
			}

			// Create body value
			body := testCase.body()

			originalBody := pcommon.NewValueEmpty()
			hasOriginalBody := false
			if testCase.originalBody != nil {
				originalBody = testCase.originalBody()
				hasOriginalBody = true
			}

			// Process body
			bodyStr, bodyJSONStr, promotedStr := exporter.processBody(context.Background(), body, originalBody, hasOriginalBody)

			err = exporter.Shutdown(context.Background())
			require.NoError(t, err)

			// Verify results
			assert.Equal(t, testCase.expectedBody, bodyStr, "body string mismatch")
			assert.Equal(t, testCase.expectedBodyJSON, bodyJSONStr, "bodyJSON string mismatch")
			assert.Equal(t, testCase.expectedPromoted, promotedStr, "promoted string mismatch")
		})
	}
}

func TestProcessBodyNonMapCounter(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	opts := testOptions(t)
	opts = append(opts,
		WithClickHouseClient(nil),
		WithNewUsageCollector(uuid.New(), nil),
		WithMeter(provider.Meter(metadata.ScopeName)),
	)

	exp, err := newExporter(
		exporter.Settings{},
		&Config{
			DSN:                       "clickhouse://localhost:9000/test",
			BodyJSONEnabled:           true,
			PromotedPathsSyncInterval: utils.ToPointer(5 * time.Minute),
			LogLevelConcurrency:       utils.ToPointer(1),
			AttributesLimits: AttributesLimits{
				FetchKeysInterval: 2 * time.Second,
				MaxDistinctValues: 25000,
			},
		},
		opts...,
	)
	require.NoError(t, err)

	ctx := context.Background()
	mapBody := pcommon.NewValueMap()
	mapBody.Map().PutStr("message", "already a map")

	exp.processBody(ctx, pcommon.NewValueStr("first"), pcommon.NewValueEmpty(), false)
	exp.processBody(ctx, pcommon.NewValueStr("second"), pcommon.NewValueEmpty(), false)
	exp.processBody(ctx, pcommon.NewValueInt(42), pcommon.NewValueEmpty(), false)
	exp.processBody(ctx, mapBody, pcommon.NewValueEmpty(), false)

	require.NoError(t, exp.Shutdown(ctx))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	var recorded int64
	var found bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "exporter_non_map_body_records" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "expected an int64 sum")
			require.Len(t, sum.DataPoints, 1)
			recorded = sum.DataPoints[0].Value
			found = true
		}
	}

	require.True(t, found, "counter was not recorded")
	assert.Equal(t, int64(3), recorded)
}

func TestGetJSONColumnBodyEmptyMap(t *testing.T) {
	body := pcommon.NewValueMap()
	b, err := json.Marshal(utils.SanitizeJSON(body.Map().AsRaw()))
	require.NoError(t, err)

	assert.Equal(t, string(b), getJSONColumnBody(body))
}


func TestExporterPushLogsDataWritesTenantScopedTagMetadata(t *testing.T) {
	mock, err := cmock.NewClickHouseWithQueryMatcher(nil, sqlmock.QueryMatcherRegexp)
	require.NoError(t, err)

	tagV2 := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_tag_attributes_v2")
	tenantTagV3 := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_tag_attributes_v3")
	attributeKeysV1 := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_logs_attribute_keys")
	resourceKeysV1 := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_logs_resource_keys")
	attributeKeysV2 := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_logs_attribute_keys_v2")
	resourceKeysV2 := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_logs_resource_keys_v2")
	logsV2 := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_logs_v2.*")
	logsResourceV2 := mock.ExpectPrepareBatch("INSERT INTO signoz_logs.distributed_logs_v2_resource.*")

	tenantTagV3.ExpectAppend()
	attributeKeysV2.ExpectAppend()
	resourceKeysV2.ExpectAppend()
	logsV2.ExpectAppend()
	logsResourceV2.ExpectAppend()

	tagV2.ExpectSend()
	tenantTagV3.ExpectSend()
	attributeKeysV1.ExpectSend()
	resourceKeysV1.ExpectSend()
	attributeKeysV2.ExpectSend()
	resourceKeysV2.ExpectSend()
	logsV2.ExpectSend()
	logsResourceV2.ExpectSend()

	mock.ExpectExec(".*insert into signoz_logs.distributed_usage.*").WithArgs()
	mock.ExpectClose()

	exporter := setupTestExporter(t, mock)
	logs := plogsgen.Generate()
	resourceAttrs := logs.ResourceLogs().At(0).Resource().Attributes()
	resourceAttrs.PutStr("signoz.org.id", "11111111-1111-4111-8111-111111111111")
	resourceAttrs.PutStr("signoz.project.id", "22222222-2222-4222-8222-222222222222")
	resourceAttrs.PutStr("tenant.resource", "resource-value")
	logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().PutStr("tenant.attribute", "attribute-value")

	require.NoError(t, exporter.pushLogsData(context.Background(), logs))
	require.NoError(t, exporter.Shutdown(context.Background()))
	require.Eventually(t, func() bool {
		return mock.ExpectationsWereMet() == nil
	}, 10*time.Second, 100*time.Millisecond)
}


// tenantMetadataCaptureBatch captures key-metadata rows without requiring ClickHouse.
type tenantMetadataCaptureBatch struct {
	driver.Batch
	rows [][]any
}

func (b *tenantMetadataCaptureBatch) Append(values ...any) error {
	row := append([]any(nil), values...)
	b.rows = append(b.rows, row)
	return nil
}

func TestTenantAttributeKeyMetadataIsIsolatedByOrganizationAndProject(t *testing.T) {
	cache := ttlcache.New(
		ttlcache.WithTTL[string, struct{}](240*time.Minute),
		ttlcache.WithCapacity[string, struct{}](50000),
	)
	defer cache.Stop()

	exp := &clickhouseLogsExporter{
		logger:    zap.NewNop(),
		keysCache: cache,
	}
	batch := &tenantMetadataCaptureBatch{}
	deduper := tagdedup.New()

	const (
		orgA     = "11111111-1111-4111-8111-111111111111"
		orgB     = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
		project1 = "22222222-2222-4222-8222-222222222222"
		project2 = "33333333-3333-4333-8333-333333333333"
	)

	// The same attribute key must be registered independently for each tenant.
	for _, tenant := range [][2]string{
		{orgA, project1},
		{orgA, project2},
		{orgB, project1},
	} {
		exp.addAttrsToAttributeKeysStatement(
			nil, nil, batch, nil,
			tenant[0], tenant[1],
			"http.method", utils.TagTypeAttribute, utils.FieldDataTypeString, deduper,
		)
	}

	// A repeated key in the same tenant must still be deduplicated.
	exp.addAttrsToAttributeKeysStatement(
		nil, nil, batch, nil,
		orgA, project1,
		"http.method", utils.TagTypeAttribute, utils.FieldDataTypeString, deduper,
	)

	require.Len(t, batch.rows, 3)
	require.Equal(t, []any{orgA, project1, "http.method", utils.FieldDataTypeString}, batch.rows[0])
	require.Equal(t, []any{orgA, project2, "http.method", utils.FieldDataTypeString}, batch.rows[1])
	require.Equal(t, []any{orgB, project1, "http.method", utils.FieldDataTypeString}, batch.rows[2])
}

func TestTenantResourceKeyMetadataIsIsolatedByOrganizationAndProject(t *testing.T) {
	cache := ttlcache.New(
		ttlcache.WithTTL[string, struct{}](240*time.Minute),
		ttlcache.WithCapacity[string, struct{}](50000),
	)
	defer cache.Stop()

	exp := &clickhouseLogsExporter{
		logger:    zap.NewNop(),
		keysCache: cache,
	}
	batch := &tenantMetadataCaptureBatch{}
	deduper := tagdedup.New()

	const (
		orgA     = "11111111-1111-4111-8111-111111111111"
		orgB     = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
		project1 = "22222222-2222-4222-8222-222222222222"
		project2 = "33333333-3333-4333-8333-333333333333"
	)

	// Resource keys must be registered independently for each organization/project.
	for _, tenant := range [][2]string{
		{orgA, project1},
		{orgA, project2},
		{orgB, project1},
	} {
		exp.addAttrsToAttributeKeysStatement(
			nil, nil, nil, batch,
			tenant[0], tenant[1],
			"service.name", utils.TagTypeResource, utils.FieldDataTypeString, deduper,
		)
	}

	// A repeated resource key in the same tenant must be deduplicated.
	exp.addAttrsToAttributeKeysStatement(
		nil, nil, nil, batch,
		orgA, project1,
		"service.name", utils.TagTypeResource, utils.FieldDataTypeString, deduper,
	)

	require.Len(t, batch.rows, 3)
	require.Equal(t, []any{orgA, project1, "service.name", utils.FieldDataTypeString}, batch.rows[0])
	require.Equal(t, []any{orgA, project2, "service.name", utils.FieldDataTypeString}, batch.rows[1])
	require.Equal(t, []any{orgB, project1, "service.name", utils.FieldDataTypeString}, batch.rows[2])
}

func TestMixedTenantAttributeAndResourceMetadataStayProjectScoped(t *testing.T) {
	cache := ttlcache.New(
		ttlcache.WithTTL[string, struct{}](240*time.Minute),
		ttlcache.WithCapacity[string, struct{}](50000),
	)
	defer cache.Stop()

	exp := &clickhouseLogsExporter{logger: zap.NewNop(), keysCache: cache}
	legacyTags := &tenantMetadataCaptureBatch{}
	tenantTags := &tenantMetadataCaptureBatch{}
	legacyAttributeKeys := &tenantMetadataCaptureBatch{}
	legacyResourceKeys := &tenantMetadataCaptureBatch{}
	tenantAttributeKeys := &tenantMetadataCaptureBatch{}
	tenantResourceKeys := &tenantMetadataCaptureBatch{}
	deduper := tagdedup.New()

	const org = "11111111-1111-4111-8111-111111111111"
	const projectA = "22222222-2222-4222-8222-222222222222"
	const projectB = "33333333-3333-4333-8333-333333333333"

	for _, project := range []string{projectA, projectB} {
		attrs := attributeMap{
			StringData: map[string]string{"deployment.environment": "production"},
			NumberData: map[string]float64{},
			BoolData:   map[string]bool{},
		}
		for _, tagType := range []utils.TagType{utils.TagTypeAttribute, utils.TagTypeResource} {
			require.NoError(t, exp.addAttrsToTagStatement(
				legacyTags, tenantTags,
				legacyAttributeKeys, legacyResourceKeys,
				tenantAttributeKeys, tenantResourceKeys,
				org, project, tagType, attrs, map[string]shouldSkipKey{}, deduper,
			))
		}
	}

	// The same key/value pair is independently emitted for each project and tag type.
	require.Len(t, tenantTags.rows, 4)
	require.Len(t, tenantAttributeKeys.rows, 2)
	require.Len(t, tenantResourceKeys.rows, 2)
	require.Empty(t, legacyTags.rows)
	require.Empty(t, legacyAttributeKeys.rows)
	require.Empty(t, legacyResourceKeys.rows)

	require.Equal(t, []any{org, projectA, "deployment.environment", utils.FieldDataTypeString}, tenantAttributeKeys.rows[0])
	require.Equal(t, []any{org, projectB, "deployment.environment", utils.FieldDataTypeString}, tenantAttributeKeys.rows[1])
	require.Equal(t, []any{org, projectA, "deployment.environment", utils.FieldDataTypeString}, tenantResourceKeys.rows[0])
	require.Equal(t, []any{org, projectB, "deployment.environment", utils.FieldDataTypeString}, tenantResourceKeys.rows[1])
}

func TestTenantCardinalitySkipIsScopedToOrganizationAndProject(t *testing.T) {
	cache := ttlcache.New(
		ttlcache.WithTTL[string, struct{}](240*time.Minute),
		ttlcache.WithCapacity[string, struct{}](50000),
	)
	defer cache.Stop()

	exp := &clickhouseLogsExporter{logger: zap.NewNop(), keysCache: cache}
	legacyTags := &tenantMetadataCaptureBatch{}
	tenantTags := &tenantMetadataCaptureBatch{}
	legacyAttributeKeys := &tenantMetadataCaptureBatch{}
	legacyResourceKeys := &tenantMetadataCaptureBatch{}
	tenantAttributeKeys := &tenantMetadataCaptureBatch{}
	tenantResourceKeys := &tenantMetadataCaptureBatch{}
	deduper := tagdedup.New()

	const org = "11111111-1111-4111-8111-111111111111"
	const projectA = "22222222-2222-4222-8222-222222222222"
	const projectB = "33333333-3333-4333-8333-333333333333"
	const attrKey = "http.route"

	// Simulate ClickHouse reporting that this key exceeded the cardinality
	// threshold in project A only. Project B must remain eligible to ingest it.
	scopedKey := org + "\x1f" + projectA + "\x1f" +
		utils.MakeKeyForAttributeKeys(attrKey, utils.TagTypeAttribute, utils.FieldDataTypeString)
	shouldSkipKeys := map[string]shouldSkipKey{
		scopedKey: {
			OrgID:       org,
			ProjectID:   projectA,
			TagKey:      attrKey,
			TagType:     string(utils.TagTypeAttribute),
			TagDataType: string(utils.FieldDataTypeString),
			StringCount: 25001,
		},
	}

	for _, project := range []string{projectA, projectB} {
		attrs := attributeMap{
			StringData: map[string]string{attrKey: "/checkout"},
			NumberData: map[string]float64{},
			BoolData:   map[string]bool{},
		}
		require.NoError(t, exp.addAttrsToTagStatement(
			legacyTags, tenantTags,
			legacyAttributeKeys, legacyResourceKeys,
			tenantAttributeKeys, tenantResourceKeys,
			org, project, utils.TagTypeAttribute, attrs, shouldSkipKeys, deduper,
		))
	}

	// Project A is skipped, while the same key/value is ingested for project B.
	require.Len(t, tenantTags.rows, 1)
	unixMilli := (time.Now().UnixMilli() / 3600000) * 3600000
	require.Equal(t, []any{org, projectB, unixMilli, attrKey, utils.TagTypeAttribute, utils.FieldDataTypeString, "/checkout", nil}, tenantTags.rows[0])
	require.Len(t, tenantAttributeKeys.rows, 2)
	require.Empty(t, legacyTags.rows)
	require.Empty(t, legacyAttributeKeys.rows)
	require.Empty(t, legacyResourceKeys.rows)
}

func TestTenantMetadataCacheIsIsolatedByOrganizationAndProject(t *testing.T) {
	cache := ttlcache.New(
		ttlcache.WithTTL[string, struct{}](240*time.Minute),
		ttlcache.WithCapacity[string, struct{}](50000),
	)
	defer cache.Stop()

	exp := &clickhouseLogsExporter{logger: zap.NewNop(), keysCache: cache}
	const org = "11111111-1111-4111-8111-111111111111"
	const projectA = "22222222-2222-4222-8222-222222222222"
	const projectB = "33333333-3333-4333-8333-333333333333"

	batchA := &tenantMetadataCaptureBatch{}
	exp.addAttrsToAttributeKeysStatement(nil, nil, nil, batchA, org, projectA,
		"service.name", utils.TagTypeResource, utils.FieldDataTypeString, tagdedup.New())
	require.Len(t, batchA.rows, 1)

	// A fresh batch/deduper for the same tenant must hit the shared cache.
	repeatedBatchA := &tenantMetadataCaptureBatch{}
	exp.addAttrsToAttributeKeysStatement(nil, nil, nil, repeatedBatchA, org, projectA,
		"service.name", utils.TagTypeResource, utils.FieldDataTypeString, tagdedup.New())
	require.Empty(t, repeatedBatchA.rows)

	// The same key in a different project must not be suppressed by project A's cache entry.
	batchB := &tenantMetadataCaptureBatch{}
	exp.addAttrsToAttributeKeysStatement(nil, nil, nil, batchB, org, projectB,
		"service.name", utils.TagTypeResource, utils.FieldDataTypeString, tagdedup.New())
	require.Len(t, batchB.rows, 1)
	require.Equal(t, []any{org, projectA, "service.name", utils.FieldDataTypeString}, batchA.rows[0])
	require.Equal(t, []any{org, projectB, "service.name", utils.FieldDataTypeString}, batchB.rows[0])
}
