package multisite

// nativeDoc is a read view over a native radosgw-admin JSON object as
// syncJSON decodes it. The underlying map stays the exact document that is
// compared and written back, so members the fixture does not read survive.
// Compare and store documents as map[string]any, never as nativeDoc:
// reflect.DeepEqual treats the two types as different. Its methods only read,
// and only by exact member name: encoding/json has already kept the last of
// repeated keys, and no case-insensitive match is made.
type nativeDoc map[string]any

// nativeString is a native JSON member that is meaningful only as a string. A
// member that is missing, null or of another type is unset, so a comparison
// with it fails.
type nativeString struct {
	value string
	set   bool
}

// is reports whether the member is the string want.
func (s nativeString) is(want string) bool { return s.set && s.value == want }

// member reads the string member key.
func (d nativeDoc) member(key string) nativeString {
	value, ok := d[key].(string)
	return nativeString{value: value, set: ok}
}

// text reads an optional string member: missing or null is "", and ok is
// false only for a member of another type.
func (d nativeDoc) text(key string) (value string, ok bool) {
	switch v := d[key].(type) {
	case nil:
		return "", true
	case string:
		return v, true
	default:
		return "", false
	}
}

// texts reads an optional array of strings: missing or null is nil, and ok is
// false for another type or a non-string element.
func (d nativeDoc) texts(key string) ([]string, bool) {
	switch v := d[key].(type) {
	case nil:
		return nil, true
	case []any:
		values := make([]string, len(v))
		for i, element := range v {
			text, ok := element.(string)
			if !ok {
				return nil, false
			}
			values[i] = text
		}
		return values, true
	default:
		return nil, false
	}
}

// object returns the object member key, or nil.
func (d nativeDoc) object(key string) nativeDoc {
	value, _ := d[key].(map[string]any)
	return value
}

// objects returns the object elements of the array member key, in order;
// elements of another type are skipped.
func (d nativeDoc) objects(key string) []nativeDoc {
	values, _ := d[key].([]any)
	result := make([]nativeDoc, 0, len(values))
	for _, value := range values {
		if object, ok := value.(map[string]any); ok {
			result = append(result, object)
		}
	}
	return result
}

// identified returns the first object element of the array member key whose
// "id" is id, or nil.
func (d nativeDoc) identified(key, id string) nativeDoc {
	for _, object := range d.objects(key) {
		if object.member("id").is(id) {
			return object
		}
	}
	return nil
}
