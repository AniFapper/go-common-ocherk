package marshaller

import "fmt"

type Raw struct{}

func (Raw) Marshal(v any) ([]byte, error) {
	switch val := v.(type) {
	case []byte:
		return val, nil
	case string:
		return []byte(val), nil
	default:
		return nil, fmt.Errorf("marshaller raw: unsupported type %T, expected []byte or string", v)
	}
}

func (Raw) Unmarshal(data []byte, v any) error {
	switch ptr := v.(type) {
	case *[]byte:
		*ptr = data
		return nil
	case *string:
		*ptr = string(data)
		return nil
	default:
		return fmt.Errorf("marshaller raw: unsupported type %T, expected *[]byte or *string", v)
	}
}
