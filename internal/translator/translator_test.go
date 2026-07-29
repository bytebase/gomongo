package translator

import (
	"errors"
	"testing"

	"github.com/bytebase/gomongo/types"
)

// TestParseSingleStatementContract pins the strict single-statement contract:
// parse errors and extra statements are rejected rather than silently dropped
// (BYT-9950: dropped statements made migrations appear to skip commands).
func TestParseSingleStatementContract(t *testing.T) {
	t.Run("valid single statement", func(t *testing.T) {
		op, err := Parse(`db.users.find({ name: "alice" })`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if op.OpType != types.OpFind {
			t.Errorf("expected OpFind, got %v", op.OpType)
		}
	})

	t.Run("comment-only input is a no-op", func(t *testing.T) {
		op, err := Parse("// just a comment\n")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if op.OpType != types.OpNoOp {
			t.Errorf("expected OpNoOp, got %v", op.OpType)
		}
	})

	t.Run("invalid statement is rejected", func(t *testing.T) {
		_, err := Parse("db.users.find({ name: })")
		var pe *ParseError
		if !errors.As(err, &pe) {
			t.Fatalf("expected *ParseError, got %T: %v", err, err)
		}
	})

	t.Run("valid statement followed by invalid is rejected", func(t *testing.T) {
		_, err := Parse("db.users.find();\nthis is not mongosh")
		var pe *ParseError
		if !errors.As(err, &pe) {
			t.Fatalf("expected *ParseError, got %T: %v", err, err)
		}
		if pe.Line != 2 {
			t.Errorf("expected error on line 2, got %d", pe.Line)
		}
	})

	t.Run("multiple valid statements are rejected", func(t *testing.T) {
		_, err := Parse("db.users.find();\ndb.orders.find();")
		var pe *ParseError
		if !errors.As(err, &pe) {
			t.Fatalf("expected *ParseError, got %T: %v", err, err)
		}
		if pe.Line != 2 {
			t.Errorf("expected error on line 2, got %d", pe.Line)
		}
	})
}

// TestParseCreateIndexArithmeticTTL pins the BYT-9950 statement: createIndex
// with a constant arithmetic expireAfterSeconds folds to an int32 TTL.
func TestParseCreateIndexArithmeticTTL(t *testing.T) {
	op, err := Parse(`db.cs_customer_frequency.createIndex(
  { trans_date: 1 },
  { expireAfterSeconds: 90 * 24 * 60 * 60, name: "cs_customer_frequency_idx2" }
);`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if op.OpType != types.OpCreateIndex {
		t.Fatalf("expected OpCreateIndex, got %v", op.OpType)
	}
	if op.IndexTTL == nil || *op.IndexTTL != 7776000 {
		t.Errorf("expected IndexTTL 7776000, got %v", op.IndexTTL)
	}
	if op.IndexName != "cs_customer_frequency_idx2" {
		t.Errorf("expected index name cs_customer_frequency_idx2, got %q", op.IndexName)
	}
}
