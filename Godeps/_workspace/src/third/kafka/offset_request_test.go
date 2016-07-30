package kafka

import "testing"

var (
	offsetRequestNoBlocks = []byte{
		0xFF, 0xFF, 0xFF, 0xFF,
		0x00, 0x00, 0x00, 0x00}

	offsetRequestOneBlock = []byte{
		0xFF, 0xFF, 0xFF, 0xFF,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x03, 'f', 'o', 'o',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x04,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x02}
)

func TestOffsetRequest(t *testing.T) {
	request := new(OffsetRequest)
	testRequest(t, "no blocks", request, offsetRequestNoBlocks)

	request.AddBlock("foo", 4, 1, 2)
	testRequest(t, "one block", request, offsetRequestOneBlock)
}