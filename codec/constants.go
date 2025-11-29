package codec

const (
	nilFlag      byte = 0xFF
	bytesFlag    byte = 0x00
	compactFlag  byte = 0x01
	intFlag      byte = 0x02
	uintFlag     byte = 0x03
	floatFlag    byte = 0x04
	decimalFlag  byte = 0x05
	durationFlag byte = 0x06
	varintFlag   byte = 0x07
	uvarintFlag  byte = 0x08
	jsonFlag     byte = 0x09
	maxFlag      byte = 0xFA
)