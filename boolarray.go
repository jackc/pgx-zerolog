package pgtype

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/jackc/pgx/pgio"
)

type BoolArray struct {
	Elements   []Bool
	Dimensions []ArrayDimension
	Status     Status
}

func (dst *BoolArray) ConvertFrom(src interface{}) error {
	switch value := src.(type) {
	case BoolArray:
		*dst = value

	case []bool:
		if value == nil {
			*dst = BoolArray{Status: Null}
		} else if len(value) == 0 {
			*dst = BoolArray{Status: Present}
		} else {
			elements := make([]Bool, len(value))
			for i := range value {
				if err := elements[i].ConvertFrom(value[i]); err != nil {
					return err
				}
			}
			*dst = BoolArray{
				Elements:   elements,
				Dimensions: []ArrayDimension{{Length: int32(len(elements)), LowerBound: 1}},
				Status:     Present,
			}
		}

	default:
		if originalSrc, ok := underlyingSliceType(src); ok {
			return dst.ConvertFrom(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Bool", value)
	}

	return nil
}

func (src *BoolArray) AssignTo(dst interface{}) error {
	switch v := dst.(type) {

	case *[]bool:
		if src.Status == Present {
			*v = make([]bool, len(src.Elements))
			for i := range src.Elements {
				if err := src.Elements[i].AssignTo(&((*v)[i])); err != nil {
					return err
				}
			}
		} else {
			*v = nil
		}

	default:
		if originalDst, ok := underlyingPtrSliceType(dst); ok {
			return src.AssignTo(originalDst)
		}
		return fmt.Errorf("cannot decode %v into %T", src, dst)
	}

	return nil
}

func (dst *BoolArray) DecodeText(src []byte) error {
	if src == nil {
		*dst = BoolArray{Status: Null}
		return nil
	}

	uta, err := ParseUntypedTextArray(string(src))
	if err != nil {
		return err
	}

	var elements []Bool

	if len(uta.Elements) > 0 {
		elements = make([]Bool, len(uta.Elements))

		for i, s := range uta.Elements {
			var elem Bool
			var elemSrc []byte
			if s != "NULL" {
				elemSrc = []byte(s)
			}
			err = elem.DecodeText(elemSrc)
			if err != nil {
				return err
			}

			elements[i] = elem
		}
	}

	*dst = BoolArray{Elements: elements, Dimensions: uta.Dimensions, Status: Present}

	return nil
}

func (dst *BoolArray) DecodeBinary(src []byte) error {
	if src == nil {
		*dst = BoolArray{Status: Null}
		return nil
	}

	var arrayHeader ArrayHeader
	rp, err := arrayHeader.DecodeBinary(src)
	if err != nil {
		return err
	}

	if len(arrayHeader.Dimensions) == 0 {
		*dst = BoolArray{Dimensions: arrayHeader.Dimensions, Status: Present}
		return nil
	}

	elementCount := arrayHeader.Dimensions[0].Length
	for _, d := range arrayHeader.Dimensions[1:] {
		elementCount *= d.Length
	}

	elements := make([]Bool, elementCount)

	for i := range elements {
		elemLen := int(int32(binary.BigEndian.Uint32(src[rp:])))
		rp += 4
		var elemSrc []byte
		if elemLen >= 0 {
			elemSrc = src[rp : rp+elemLen]
			rp += elemLen
		}
		err = elements[i].DecodeBinary(elemSrc)
		if err != nil {
			return err
		}
	}

	*dst = BoolArray{Elements: elements, Dimensions: arrayHeader.Dimensions, Status: Present}
	return nil
}

func (src *BoolArray) EncodeText(w io.Writer) error {
	if done, err := encodeNotPresent(w, src.Status); done {
		return err
	}

	if len(src.Dimensions) == 0 {
		_, err := pgio.WriteInt32(w, 2)
		if err != nil {
			return err
		}

		_, err = w.Write([]byte("{}"))
		return err
	}

	buf := &bytes.Buffer{}

	err := EncodeTextArrayDimensions(buf, src.Dimensions)
	if err != nil {
		return err
	}

	// dimElemCounts is the multiples of elements that each array lies on. For
	// example, a single dimension array of length 4 would have a dimElemCounts of
	// [4]. A multi-dimensional array of lengths [3,5,2] would have a
	// dimElemCounts of [30,10,2]. This is used to simplify when to render a '{'
	// or '}'.
	dimElemCounts := make([]int, len(src.Dimensions))
	dimElemCounts[len(src.Dimensions)-1] = int(src.Dimensions[len(src.Dimensions)-1].Length)
	for i := len(src.Dimensions) - 2; i > -1; i-- {
		dimElemCounts[i] = int(src.Dimensions[i].Length) * dimElemCounts[i+1]
	}

	textElementWriter := NewTextElementWriter(buf)

	for i, elem := range src.Elements {
		if i > 0 {
			err = pgio.WriteByte(buf, ',')
			if err != nil {
				return err
			}
		}

		for _, dec := range dimElemCounts {
			if i%dec == 0 {
				err = pgio.WriteByte(buf, '{')
				if err != nil {
					return err
				}
			}
		}

		textElementWriter.Reset()
		err = elem.EncodeText(textElementWriter)
		if err != nil {
			return err
		}

		for _, dec := range dimElemCounts {
			if (i+1)%dec == 0 {
				err = pgio.WriteByte(buf, '}')
				if err != nil {
					return err
				}
			}
		}
	}

	_, err = pgio.WriteInt32(w, int32(buf.Len()))
	if err != nil {
		return err
	}

	_, err = buf.WriteTo(w)
	return err
}

func (src *BoolArray) EncodeBinary(w io.Writer) error {
	return src.encodeBinary(w, BoolOID)
}

func (src *BoolArray) encodeBinary(w io.Writer, elementOID int32) error {
	if done, err := encodeNotPresent(w, src.Status); done {
		return err
	}

	var arrayHeader ArrayHeader

	// TODO - consider how to avoid having to buffer array before writing length -
	// or how not pay allocations for the byte order conversions.
	elemBuf := &bytes.Buffer{}

	for i := range src.Elements {
		err := src.Elements[i].EncodeBinary(elemBuf)
		if err != nil {
			return err
		}
		if src.Elements[i].Status == Null {
			arrayHeader.ContainsNull = true
		}
	}

	arrayHeader.ElementOID = elementOID
	arrayHeader.Dimensions = src.Dimensions

	// TODO - consider how to avoid having to buffer array before writing length -
	// or how not pay allocations for the byte order conversions.
	headerBuf := &bytes.Buffer{}
	err := arrayHeader.EncodeBinary(headerBuf)
	if err != nil {
		return err
	}

	_, err = pgio.WriteInt32(w, int32(headerBuf.Len()+elemBuf.Len()))
	if err != nil {
		return err
	}

	_, err = headerBuf.WriteTo(w)
	if err != nil {
		return err
	}

	_, err = elemBuf.WriteTo(w)
	if err != nil {
		return err
	}

	return err
}
