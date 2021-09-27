package ethlogscanner

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

// Cursor holds the exact value of the log location on the blockchain.
//
// Multiple log events can be emitted by a specific transaction in a specific block.
// To prevent duplicate processing while tracking logs on the blockchain, we should
// maintain the sequence number of the last processed log event with the sequence number
// of transaction emits the event, and the block number of the transaction.
//
// In fact, the logs are relative to the block, not the transaction. But here we want to
// keep the transaction index to avoid the lack of information and quick access to a
// transaction using Cursor.
//
// Cursor is a 64-bit unsigned integer consisting of a 30-bit block number,
// a 14-bit transaction index, and a 20-bit log index. Standard math operators can be
// used for comparing cursor positions. We assume the block number is can not greater
// than 2^30, the number of transactions in a block is can not greater than 2^14, and
// the number of log events in a block is can not greater than 2^20.

// It also supports JSON serialization to provide a semantic representation of the data.
type Cursor uint64

type cursor struct {
	BlockNum uint64 `json:"block_num"`
	TxIndex  uint   `json:"tx_index"`
	LogIndex uint   `json:"log_index"`
}

const (
	bitsBlockNum = 30 // max 1.073.741.824 blocks in the chain
	bitsTxIndex  = 14 // max 16.384 transactions in a block
	bitsLogIndex = 20 // max 1.048.576 logs in a block

	maxBlockNum        = uint64(1<<bitsBlockNum) - 1
	maxTxIndexInBlock  = uint64(1<<bitsTxIndex) - 1
	maxLogIndexInBlock = uint64(1<<bitsLogIndex) - 1
)

func init() {
	// assert no overflow bug
	if bitsBlockNum+bitsTxIndex+bitsLogIndex > 64 {
		panic("bug: Cursor can not represent as uint64")
	}
}

// MakeCursor returns a Cursor for a specific log location on the blockchain.
func MakeCursor(blockNum uint64, txIndex, logIndex uint) Cursor {
	if blockNum > maxBlockNum {
		panic(fmt.Sprintf("blockNum can not be greater than %d", maxBlockNum))
	}

	if uint64(txIndex) > maxTxIndexInBlock {
		panic(fmt.Sprintf("txIndex can not be greater than %d", maxTxIndexInBlock))
	}

	if uint64(logIndex) > maxLogIndexInBlock {
		panic(fmt.Sprintf("logIndex can not be greater than %d", maxLogIndexInBlock))
	}

	l := uint64(blockNum << (bitsTxIndex + bitsLogIndex))
	l |= uint64(txIndex << bitsLogIndex)
	l |= uint64(logIndex)

	return Cursor(l)
}

func (l Cursor) BlockNum() uint64 {
	return uint64(l >> (bitsTxIndex + bitsLogIndex))
}

func (l Cursor) TxIndex() uint {
	return uint((uint64(l) >> bitsLogIndex) & maxTxIndexInBlock)
}

func (l Cursor) LogIndex() uint {
	return uint(uint64(l) & maxLogIndexInBlock)
}

func (c Cursor) String() string {
	return fmt.Sprintf("%d[%d].%d", c.BlockNum(), c.TxIndex(), c.LogIndex())
}

func (c Cursor) Next() Cursor {
	return MakeCursor(c.BlockNum(), c.TxIndex(), c.LogIndex()+1)
}

func (c Cursor) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(cursor{
		BlockNum: c.BlockNum(),
		TxIndex:  c.TxIndex(),
		LogIndex: c.LogIndex(),
	})

	return b, errors.WithStack(err)
}

func (c *Cursor) UnmarshalJSON(b []byte) error {
	m := cursor{}

	if err := json.Unmarshal(b, &m); err != nil {
		return errors.WithStack(err)
	}

	*c = MakeCursor(m.BlockNum, m.TxIndex, m.LogIndex)

	return nil
}
