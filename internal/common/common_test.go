package common

import (
	"hash/crc32"
	"testing"
)

// ============================================================
// types.go: CanonicalKey, ExtractBaseKey, IsSaltedKey, CreateSaltedKey
// ============================================================

func TestCanonicalKey_StringKey(t *testing.T) {
	got, err := CanonicalKey("hello")
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Errorf("expected 'hello', got %q", got)
	}
}

func TestCanonicalKey_SaltedKey(t *testing.T) {
	// JSON unmarshaling produces float64 for numbers
	key := []interface{}{"the", float64(3)}
	got, err := CanonicalKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if got != "the#3" {
		t.Errorf("expected 'the#3', got %q", got)
	}
}

func TestCanonicalKey_SaltZero(t *testing.T) {
	key := []interface{}{"word", float64(0)}
	got, err := CanonicalKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if got != "word#0" {
		t.Errorf("expected 'word#0', got %q", got)
	}
}

func TestCanonicalKey_InvalidType(t *testing.T) {
	_, err := CanonicalKey(42)
	if err == nil {
		t.Error("expected error for unsupported key type, got nil")
	}
}

func TestExtractBaseKey_String(t *testing.T) {
	got, err := ExtractBaseKey("hello")
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Errorf("expected 'hello', got %q", got)
	}
}

func TestExtractBaseKey_SaltedKey(t *testing.T) {
	key := []interface{}{"the", float64(5)}
	got, err := ExtractBaseKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if got != "the" {
		t.Errorf("expected 'the', got %q", got)
	}
}

func TestExtractBaseKey_StripsSalt(t *testing.T) {
	// Different salt values should all return the same base key
	for _, salt := range []float64{0, 1, 2, 7} {
		key := []interface{}{"fox", salt}
		got, err := ExtractBaseKey(key)
		if err != nil {
			t.Fatalf("salt=%.0f: unexpected error: %v", salt, err)
		}
		if got != "fox" {
			t.Errorf("salt=%.0f: expected 'fox', got %q", salt, got)
		}
	}
}

func TestIsSaltedKey_String(t *testing.T) {
	if IsSaltedKey("hello") {
		t.Error("string key should not be salted")
	}
}

func TestIsSaltedKey_Array(t *testing.T) {
	if !IsSaltedKey([]interface{}{"hello", float64(0)}) {
		t.Error("array key should be salted")
	}
}

func TestCreateSaltedKey_ReturnsArray(t *testing.T) {
	key := CreateSaltedKey("the", 3)
	arr, ok := key.([]interface{})
	if !ok {
		t.Fatalf("expected []interface{}, got %T", key)
	}
	if len(arr) != 2 {
		t.Fatalf("expected length 2, got %d", len(arr))
	}
	if arr[0] != "the" {
		t.Errorf("expected word 'the', got %v", arr[0])
	}
	if arr[1] != 3 {
		t.Errorf("expected salt 3, got %v", arr[1])
	}
}

func TestCreateSaltedKey_IsSalted(t *testing.T) {
	key := CreateSaltedKey("word", 0)
	if !IsSaltedKey(key) {
		t.Error("created salted key should be recognized as salted")
	}
}

// ============================================================
// hashing.go: PartitionKey
// ============================================================

func TestPartitionKey_InRange(t *testing.T) {
	R := 8
	for _, key := range []string{"the", "hello", "world", "a", "z"} {
		p, err := PartitionKey(key, R)
		if err != nil {
			t.Fatalf("key=%q: unexpected error: %v", key, err)
		}
		if p < 0 || p >= R {
			t.Errorf("key=%q: partition %d out of range [0, %d)", key, p, R)
		}
	}
}

func TestPartitionKey_Deterministic(t *testing.T) {
	key := "the"
	R := 8
	p1, _ := PartitionKey(key, R)
	p2, _ := PartitionKey(key, R)
	if p1 != p2 {
		t.Errorf("partitioning is not deterministic: got %d then %d", p1, p2)
	}
}

func TestPartitionKey_MatchesCRC32(t *testing.T) {
	// Verify hash matches expected CRC32 computation
	key := "the"
	R := 8
	expected := int(crc32.ChecksumIEEE([]byte(key))) % R
	got, err := PartitionKey(key, R)
	if err != nil {
		t.Fatal(err)
	}
	if got != expected {
		t.Errorf("expected partition %d (crc32), got %d", expected, got)
	}
}

func TestPartitionKey_SaltedKeyInRange(t *testing.T) {
	R := 8
	// int salt (from CreateSaltedKey)
	key := CreateSaltedKey("the", 2)
	p, err := PartitionKey(key, R)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p < 0 || p >= R {
		t.Errorf("salted key partition %d out of range [0, %d)", p, R)
	}
}

func TestPartitionKey_SaltSpreadLoad(t *testing.T) {
	// With enough splits, a heavy key's salted variants should not ALL land
	// on the same reducer (they should spread across at least 2 reducers for splits>=2).
	R := 8
	splits := R
	partitions := map[int]bool{}
	for s := 0; s < splits; s++ {
		key := CreateSaltedKey("the", s)
		p, err := PartitionKey(key, R)
		if err != nil {
			t.Fatalf("s=%d: unexpected error: %v", s, err)
		}
		partitions[p] = true
	}
	if len(partitions) < 2 {
		t.Errorf("expected salted splits to land on multiple reducers, only got %d distinct partitions", len(partitions))
	}
}

func TestPartitionKey_UnsaltedVsSaltedDiffer(t *testing.T) {
	// "the" and ["the", 0] should hash to different canonical strings
	// and likely different reducers (not guaranteed, but canonical keys differ)
	R := 8
	unsalted, err := PartitionKey("the", R)
	if err != nil {
		t.Fatal(err)
	}
	// "the" hashes as []byte("the"), ["the",0] hashes as []byte("the#0")
	salted, err := PartitionKey(CreateSaltedKey("the", 0), R)
	if err != nil {
		t.Fatal(err)
	}
	// They could collide by chance, but we verify they represent different inputs
	expectedUnsalted := int(crc32.ChecksumIEEE([]byte("the"))) % R
	expectedSalted := int(crc32.ChecksumIEEE([]byte("the#0"))) % R
	if unsalted != expectedUnsalted {
		t.Errorf("unsalted: expected %d, got %d", expectedUnsalted, unsalted)
	}
	if salted != expectedSalted {
		t.Errorf("salted: expected %d, got %d", expectedSalted, salted)
	}
}

func TestPartitionKey_InvalidType(t *testing.T) {
	_, err := PartitionKey(42, 8)
	if err == nil {
		t.Error("expected error for unsupported key type")
	}
}

// ============================================================
// metrics.go: ComputeCoV, ComputeMaxMedianRatio
// ============================================================

func TestComputeCoV_Uniform(t *testing.T) {
	// All equal values → std = 0 → CoV = 0
	values := []int64{100, 100, 100, 100}
	cov := ComputeCoV(values)
	if cov != 0.0 {
		t.Errorf("expected CoV=0 for uniform load, got %f", cov)
	}
}

func TestComputeCoV_Skewed(t *testing.T) {
	// One reducer has 10x the load of others → high CoV
	values := []int64{1000, 100, 100, 100}
	cov := ComputeCoV(values)
	if cov <= 0 {
		t.Errorf("expected CoV > 0 for skewed load, got %f", cov)
	}
}

func TestComputeCoV_Empty(t *testing.T) {
	cov := ComputeCoV([]int64{})
	if cov != 0.0 {
		t.Errorf("expected CoV=0 for empty slice, got %f", cov)
	}
}

func TestComputeCoV_MitigationLowerThanBaseline(t *testing.T) {
	// After skew mitigation, CoV should be meaningfully lower
	baseline := []int64{1000, 80, 90, 75, 85, 70, 80, 95}
	mitigated := []int64{200, 195, 190, 185, 180, 175, 180, 195}
	covBaseline := ComputeCoV(baseline)
	covMitigated := ComputeCoV(mitigated)
	if covMitigated >= covBaseline {
		t.Errorf("expected mitigated CoV (%f) < baseline CoV (%f)", covMitigated, covBaseline)
	}
}

func TestComputeMaxMedianRatio_Uniform(t *testing.T) {
	// All equal → max/median = 1
	values := []int64{50, 50, 50, 50}
	ratio := ComputeMaxMedianRatio(values)
	if ratio != 1.0 {
		t.Errorf("expected ratio=1.0 for uniform load, got %f", ratio)
	}
}

func TestComputeMaxMedianRatio_Skewed(t *testing.T) {
	values := []int64{10, 10, 10, 100}
	ratio := ComputeMaxMedianRatio(values)
	if ratio <= 1.0 {
		t.Errorf("expected ratio > 1.0 for skewed load, got %f", ratio)
	}
}

func TestComputeMaxMedianRatio_Empty(t *testing.T) {
	ratio := ComputeMaxMedianRatio([]int64{})
	if ratio != 0.0 {
		t.Errorf("expected 0 for empty slice, got %f", ratio)
	}
}

func TestComputeMaxMedianRatio_MitigationLowerThanBaseline(t *testing.T) {
	baseline := []int64{1000, 80, 90, 75, 85, 70, 80, 95}
	mitigated := []int64{200, 195, 190, 185, 180, 175, 180, 195}
	ratioBaseline := ComputeMaxMedianRatio(baseline)
	ratioMitigated := ComputeMaxMedianRatio(mitigated)
	if ratioMitigated >= ratioBaseline {
		t.Errorf("expected mitigated ratio (%f) < baseline ratio (%f)", ratioMitigated, ratioBaseline)
	}
}
