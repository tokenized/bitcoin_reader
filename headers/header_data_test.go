package headers

import (
	"bytes"
	"encoding/json"
	"math/big"
	"math/rand"
	"testing"
	"time"

	"github.com/tokenized/pkg/bitcoin"
)

func Test_HeaderData_Serialize(t *testing.T) {
	genesis := genesisHeader(bitcoin.MainNet)
	data := &HeaderData{
		Hash:            *genesis.BlockHash(),
		Header:          genesis,
		AccumulatedWork: bitcoin.ConvertToWork(bitcoin.ConvertToDifficulty(genesis.Bits)),
	}

	buf := &bytes.Buffer{}
	if err := data.Serialize(buf); err != nil {
		t.Fatalf("Failed to serialize header data : %s", err)
	}

	if buf.Len() != headerDataSerializeSize {
		t.Errorf("Wrong serialize size : got %d, want %d", buf.Len(), headerDataSerializeSize)
	}

	read := &HeaderData{}
	if err := read.Deserialize(buf); err != nil {
		t.Fatalf("Failed to deserialize header data : %s", err)
	}
	js, _ := json.MarshalIndent(read, "", "  ")
	t.Logf("Read : %s", js)

	if !read.Hash.Equal(&data.Hash) {
		t.Errorf("Wrong hash : \ngot  : %s\nwant : %s", read.Hash, data.Hash)
	}

	if !read.Header.BlockHash().Equal(genesis.BlockHash()) {
		t.Errorf("Wrong header : \ngot  : %s\nwant : %s", read.Header.BlockHash(),
			genesis.BlockHash())
	}

	if read.AccumulatedWork.Cmp(data.AccumulatedWork) != 0 {
		t.Errorf("Wrong hash : \ngot  : %s\nwant : %s", read.AccumulatedWork.Text(16),
			data.AccumulatedWork.Text(16))
	}
}

func Test_serializeBigInt(t *testing.T) {
	in := &big.Int{}
	buf := &bytes.Buffer{}

	if err := serializeBigInt(in, buf); err != nil {
		t.Fatalf("Failed to serialize big int : %s", err)
	}

	out, err := deserializeBigInt(buf)
	if err != nil {
		t.Fatalf("Failed to deserialize big int : %s", err)
	}
	t.Logf("Read : %s", out.Text(16))

	if in.Cmp(out) != 0 {
		t.Errorf("Read value not equal : got %s, want %s", out.Text(16), in.Text(16))
	}

	in.Add(in, big.NewInt(0x128379826753))
	buf = &bytes.Buffer{}

	if err := serializeBigInt(in, buf); err != nil {
		t.Fatalf("Failed to serialize big int : %s", err)
	}

	out, err = deserializeBigInt(buf)
	if err != nil {
		t.Fatalf("Failed to deserialize big int : %s", err)
	}
	t.Logf("Read : %s", out.Text(16))

	if in.Cmp(out) != 0 {
		t.Errorf("Read value not equal : got %s, want %s", out.Text(16), in.Text(16))
	}

	max := &big.Int{}
	max.SetString("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 16)
	in.Rand(rand.New(rand.NewSource(time.Now().UnixNano())), max)
	buf = &bytes.Buffer{}

	if err := serializeBigInt(in, buf); err != nil {
		t.Fatalf("Failed to serialize big int : %s", err)
	}

	out, err = deserializeBigInt(buf)
	if err != nil {
		t.Fatalf("Failed to deserialize big int : %s", err)
	}
	t.Logf("Read : %s", out.Text(16))

	if in.Cmp(out) != 0 {
		t.Errorf("Read value not equal : got %s, want %s", out.Text(16), in.Text(16))
	}
}
