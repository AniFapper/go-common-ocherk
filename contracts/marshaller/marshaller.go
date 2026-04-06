package marshaller

// Marshaller defines the contract for serializing and deserializing data.
type Marshaller interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}