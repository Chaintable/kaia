// Copyright 2025 The Kaia Authors
// This file is part of the Kaia library.
//
// The Kaia library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The Kaia library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the Kaia library. If not, see <http://www.gnu.org/licenses/>.

package blockchain

import (
	"testing"

	"github.com/kaiachain/kaia/blockchain/types"
	"github.com/kaiachain/kaia/consensus/faker"
	"github.com/kaiachain/kaia/params"
	ptypes "github.com/kaiachain/kaia/pipeline/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPipelineGetCommonAncestor(t *testing.T) {
	db, bc, err := newCanonical(faker.NewFaker(), 0, true)
	require.NoError(t, err)
	defer bc.Stop()

	// main: genesis - m0 - m1 - m2 - m3 - m4
	main, _ := GenerateChain(params.TestChainConfig, bc.CurrentBlock(), faker.NewFaker(), db, 5, func(i int, b *BlockGen) {
		b.OffsetTime(1)
	})
	// fork: m1 - f0 - f1 - f2 - f3
	fork, _ := GenerateChain(params.TestChainConfig, main[1], faker.NewFaker(), db, 4, func(i int, b *BlockGen) {
		b.OffsetTime(2)
	})
	_, err = bc.InsertChain(main)
	require.NoError(t, err)
	_, err = bc.InsertChain(fork)
	require.NoError(t, err)

	ctx := func(b *types.Block) ptypes.BlockContext { return toPipelineBlockContext(b.Header()) }
	ctxs := func(bs ...*types.Block) []ptypes.BlockContext {
		out := make([]ptypes.BlockContext, len(bs))
		for i, b := range bs {
			out[i] = ctx(b)
		}
		return out
	}

	// Child of the last pushed block: just the new head.
	ancestor, drop, add := bc.getCommonAncestor(ctx(main[2]), ctx(main[3]))
	assert.Equal(t, ctx(main[2]), ancestor)
	assert.Empty(t, drop)
	assert.Equal(t, ctxs(main[3]), add)

	// Several blocks behind on the same chain: every block in between, ascending.
	ancestor, drop, add = bc.getCommonAncestor(ctx(main[0]), ctx(main[4]))
	assert.Equal(t, ctx(main[0]), ancestor)
	assert.Empty(t, drop)
	assert.Equal(t, ctxs(main[1], main[2], main[3], main[4]), add)

	// Reorg from main[4] to the longer fork: drop m2..m4, add f0..f3.
	ancestor, drop, add = bc.getCommonAncestor(ctx(main[4]), ctx(fork[3]))
	assert.Equal(t, ctx(main[1]), ancestor)
	assert.Equal(t, ctxs(main[2], main[3], main[4]), drop)
	assert.Equal(t, ctxs(fork[0], fork[1], fork[2], fork[3]), add)

	// Reorg to a fork block at the same height.
	ancestor, drop, add = bc.getCommonAncestor(ctx(main[3]), ctx(fork[1]))
	assert.Equal(t, ctx(main[1]), ancestor)
	assert.Equal(t, ctxs(main[2], main[3]), drop)
	assert.Equal(t, ctxs(fork[0], fork[1]), add)
}
