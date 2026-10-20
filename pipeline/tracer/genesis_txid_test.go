package tracer

import (
	"strings"
	"testing"

	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/common/hexutil"
)

func TestGenesisTxID(t *testing.T) {
	cases := []struct {
		kind int
		addr string
		want string
	}{
		{1, "0x0000000000000000000000000000000000000001", "0x0100000000000000000000000000000000000000000000000000000000000001"},
		{2, "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd", "0x020000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd"},
		{3, "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "0x030000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"},
	}
	for _, c := range cases {
		got := genesisTxID(c.kind, c.addr)
		if got != c.want {
			t.Fatalf("kind=%d addr=%s: got %s, want %s", c.kind, c.addr, got, c.want)
		}
		if len(got) != 66 {
			t.Fatalf("len(%s)=%d, want 66", got, len(got))
		}
		if !strings.HasPrefix(got, "0x") {
			t.Fatalf("%s missing 0x prefix", got)
		}
		// 必须是合法 hex, 可解析为 bytes32
		b, err := hexutil.Decode(got)
		if err != nil {
			t.Fatalf("%s not valid hex: %v", got, err)
		}
		if len(b) != common.HashLength {
			t.Fatalf("%s decodes to %d bytes, want %d", got, len(b), common.HashLength)
		}
		if common.HexToHash(got).Hex() != got {
			t.Fatalf("%s does not round-trip through common.Hash: %s", got, common.HexToHash(got).Hex())
		}
	}
}
