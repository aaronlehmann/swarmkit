package protobuf

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/docker/swarmkit/api"
	"github.com/docker/swarmkit/api/descriptor"
)

var (
	errInvalidLength = fmt.Errorf("proto: negative length found during unmarshaling")
	errIntOverflow   = fmt.Errorf("proto: integer overflow")
)

// Index generates indices from the given marshalled protobuf.
func (indexer *Indexer) Index(payload []byte) ([]api.IndexEntry, error) {
	var entries []api.IndexEntry

	// TODO(aaronl): This could be optimized by doing only one pass over
	// the payload.
	for _, index := range indexer.indices {
		found, err := fieldVal(payload, index, &entries)
		if err != nil {
			return nil, err
		}

		// For integer-valued and float-valued fields that are not repeated, we
		// should insert default values into the index.
		// Note: one might think we should also exclude optional values, but this
		// doesn't seem to work well in practice with proto3 (any 0-value ints
		// will be missing from the indices).
		if !found && index.finalLabel != descriptor.FieldDescriptorProto_LABEL_REPEATED {
			entries = append(entries, api.IndexEntry{Key: index.key, Val: index.defaultValue})
		}
	}

	return entries, nil
}

func fieldVal(payload []byte, protobufIndex protobufIndex, entries *[]api.IndexEntry) (bool, error) {
	found := false
	l := len(payload)
	index := 0
	for index < l {
		var (
			err  error
			wire uint64
		)
		preIndex := index
		wire, index, err = varint(payload, index)
		if err != nil {
			return found, err
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if fieldNum == protobufIndex.fieldNumberPath[0] {
			if len(protobufIndex.fieldNumberPath) == 1 {
				val, err := extractValue(payload[index:], wireType, protobufIndex.finalType)
				if err != nil {
					return found, err
				}
				*entries = append(*entries, api.IndexEntry{Key: protobufIndex.key, Val: val})
				found = true
			}
			if wireType == 2 && len(protobufIndex.fieldNumberPath) > 1 {
				var length uint64
				length, index, err = varint(payload, index)
				if err != nil {
					return found, err
				}

				if index+int(length) > l {
					return found, io.ErrUnexpectedEOF
				}

				newProtobufIndex := protobufIndex
				newProtobufIndex.fieldNumberPath = protobufIndex.fieldNumberPath[1:]
				innerFound, err := fieldVal(payload[index:index+int(length)], newProtobufIndex, entries)
				if err != nil {
					return found, err
				}
				if innerFound {
					found = true
				}
			}
		}

		index = preIndex
		skippy, err := skip(payload[index:])
		if err != nil {
			return found, err
		}
		if skippy < 0 {
			return found, errInvalidLength
		}
		if index+skippy > l {
			return found, io.ErrUnexpectedEOF
		}
		index += skippy
	}

	return found, nil
}

func varint(payload []byte, index int) (uint64, int, error) {
	var res uint64
	for shift := uint(0); ; shift += 7 {
		if shift >= 64 {
			return 0, 0, errIntOverflow
		}
		if index >= len(payload) {
			return 0, 0, io.ErrUnexpectedEOF
		}
		b := payload[index]
		index++
		res |= (uint64(b) & 0x7F) << shift
		if b < 0x80 {
			break
		}
	}

	return res, index, nil
}

func extractValue(payload []byte, wireType int, finalType descriptor.FieldDescriptorProto_Type) (string, error) {
	switch finalType {
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE: // fixed 64
		if wireType != 1 {
			return "", fmt.Errorf("proto: wrong wireType = %d for double field", wireType)
		}

		if len(payload) < 8 {
			return "", io.ErrUnexpectedEOF
		}

		bits := binary.LittleEndian.Uint64(payload)
		float := math.Float64frombits(bits)
		return strconv.FormatFloat(float, 'f', -1, 64), nil

	case descriptor.FieldDescriptorProto_TYPE_FLOAT: // fixed 32
		if wireType != 5 {
			return "", fmt.Errorf("proto: wrong wireType = %d for float field", wireType)
		}

		if len(payload) < 4 {
			return "", io.ErrUnexpectedEOF
		}

		bits := binary.LittleEndian.Uint32(payload)
		float := math.Float32frombits(bits)
		return strconv.FormatFloat(float64(float), 'f', -1, 32), nil

	case descriptor.FieldDescriptorProto_TYPE_UINT32, descriptor.FieldDescriptorProto_TYPE_UINT64: // varint
		if wireType != 0 {
			return "", fmt.Errorf("proto: wrong wireType = %d for uint32 or uint64 field", wireType)
		}

		var val uint64
		val, _, err := varint(payload, 0)
		if err != nil {
			return "", err
		}

		return strconv.FormatUint(val, 10), nil
	case descriptor.FieldDescriptorProto_TYPE_INT32, descriptor.FieldDescriptorProto_TYPE_INT64, descriptor.FieldDescriptorProto_TYPE_ENUM: // varint, not ZigZag encoded
		if wireType != 0 {
			return "", fmt.Errorf("proto: wrong wireType = %d for int32, int64, or enum field", wireType)
		}

		var val uint64
		val, _, err := varint(payload, 0)
		if err != nil {
			return "", err
		}

		return strconv.FormatInt(int64(val), 10), nil

	case descriptor.FieldDescriptorProto_TYPE_FIXED64: // fixed 64
		if wireType != 1 {
			return "", fmt.Errorf("proto: wrong wireType = %d for fixed64 field", wireType)
		}

		if len(payload) < 8 {
			return "", io.ErrUnexpectedEOF
		}

		intVal := binary.LittleEndian.Uint64(payload)
		return strconv.FormatUint(intVal, 10), nil

	case descriptor.FieldDescriptorProto_TYPE_FIXED32: // fixed 32
		if wireType != 5 {
			return "", fmt.Errorf("proto: wrong wireType = %d for fixed32 field", wireType)
		}

		if len(payload) < 8 {
			return "", io.ErrUnexpectedEOF
		}

		intVal := binary.LittleEndian.Uint32(payload)
		return strconv.FormatUint(uint64(intVal), 10), nil

	case descriptor.FieldDescriptorProto_TYPE_BOOL: // varint
		if wireType != 0 {
			return "", fmt.Errorf("proto: wrong wireType = %d for bool field", wireType)
		}

		var val uint64
		val, _, err := varint(payload, 0)
		if err != nil {
			return "", err
		}

		if val != 0 {
			return "true", nil
		}
		return "false", nil

	case descriptor.FieldDescriptorProto_TYPE_STRING, descriptor.FieldDescriptorProto_TYPE_BYTES: // Length-delimited
		if wireType != 2 {
			return "", fmt.Errorf("proto: wrong wireType = %d for string or byte field", wireType)
		}

		var length uint64
		length, index, err := varint(payload, 0)
		if err != nil {
			return "", err
		}

		if int(length)+index > len(payload) {
			return "", io.ErrUnexpectedEOF
		}

		return string(payload[index : index+int(length)]), nil

	case descriptor.FieldDescriptorProto_TYPE_SFIXED32: // fixed 32
		if wireType != 5 {
			return "", fmt.Errorf("proto: wrong wireType = %d for sfixed32 field", wireType)
		}

		if len(payload) < 4 {
			return "", io.ErrUnexpectedEOF
		}

		intVal := binary.LittleEndian.Uint32(payload)
		return strconv.FormatInt(int64(intVal), 10), nil

	case descriptor.FieldDescriptorProto_TYPE_SFIXED64: // fixed 64
		if wireType != 1 {
			return "", fmt.Errorf("proto: wrong wireType = %d for sfixed64 field", wireType)
		}

		if len(payload) < 8 {
			return "", io.ErrUnexpectedEOF
		}

		intVal := binary.LittleEndian.Uint64(payload)
		return strconv.FormatInt(int64(intVal), 10), nil

	case descriptor.FieldDescriptorProto_TYPE_SINT32: // varint, zigzag
		if wireType != 0 {
			return "", fmt.Errorf("proto: wrong wireType = %d for sint32 field", wireType)
		}

		var val uint64
		val, _, err := varint(payload, 0)
		if err != nil {
			return "", err
		}

		decodedVal := int32((uint32(val) >> 1) ^ uint32(((val&1)<<31)>>31))
		return strconv.FormatInt(int64(decodedVal), 10), nil

	case descriptor.FieldDescriptorProto_TYPE_SINT64: // varint, zigzag
		if wireType != 0 {
			return "", fmt.Errorf("proto: wrong wireType = %d for sint64 field", wireType)
		}

		var val uint64
		val, _, err := varint(payload, 0)
		if err != nil {
			return "", err
		}

		decodedVal := int64((val >> 1) ^ uint64((int64(val&1)<<63)>>63))
		return strconv.FormatInt(int64(decodedVal), 10), nil

	case descriptor.FieldDescriptorProto_TYPE_GROUP, descriptor.FieldDescriptorProto_TYPE_MESSAGE:
		return "", fmt.Errorf("unsupported field type %s", descriptor.FieldDescriptorProto_Type_name[int32(finalType)])
	default:
		return "", fmt.Errorf("unsupported field type %d", finalType)
	}
}

func skip(payload []byte) (n int, err error) {
	l := len(payload)
	index := 0
	for index < l {
		var (
			err  error
			wire uint64
		)
		wire, index, err = varint(payload, index)
		if err != nil {
			return 0, err
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, errIntOverflow
				}
				if index >= l {
					return 0, io.ErrUnexpectedEOF
				}
				index++
				if payload[index-1] < 0x80 {
					break
				}
			}
			return index, nil
		case 1:
			index += 8
			return index, nil
		case 2:
			var length uint64
			length, index, err = varint(payload, index)
			if err != nil {
				return 0, err
			}
			index += int(length)
			if length < 0 {
				return 0, errInvalidLength
			}
			return index, nil
		case 3:
			for {
				var innerWire uint64
				start := index
				innerWire, index, err = varint(payload, index)
				if err != nil {
					return 0, err
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skip(payload[start:])
				if err != nil {
					return 0, err
				}
				index = start + next
			}
			return index, nil
		case 4:
			return index, nil
		case 5:
			index += 4
			return index, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}
