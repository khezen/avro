package avro

import "github.com/valyala/fastjson"

func translateValueToMetaFields(value *fastjson.Value) (namespace, name, documentation string, aliases []string, err error) {
	if !value.Exists("name") {
		return "", "", "", nil, ErrInvalidSchema
	}
	nameBytes, err := value.Get("name").StringBytes()
	if err != nil {
		return "", "", "", nil, ErrInvalidSchema
	}
	name = string(nameBytes)
	if value.Exists("namespace") {
		namespaceBytes, err := value.Get("namespace").StringBytes()
		if err != nil {
			return "", "", "", nil, ErrInvalidSchema
		}
		namespace = string(namespaceBytes)
	}
	documentation, err = translateValueToDocumentation(value)
	if err != nil {
		return "", "", "", nil, err
	}
	if value.Exists("aliases") {
		aliasValues, err := value.Get("aliases").Array()
		if err != nil {
			return "", "", "", nil, ErrInvalidSchema
		}
		aliases = make([]string, 0, len(aliasValues))
		for _, aliasValue := range aliasValues {
			aliasStringBytes, err := aliasValue.StringBytes()
			if err != nil {
				return "", "", "", nil, ErrInvalidSchema
			}
			aliases = append(aliases, string(aliasStringBytes))
		}
	}
	return namespace, name, documentation, aliases, nil
}

func translateValueToDocumentation(value *fastjson.Value) (documentation string, err error) {
	if value.Exists("doc") {
		documentationBytes, err := value.Get("doc").StringBytes()
		if err != nil {
			return "", ErrInvalidSchema
		}
		documentation = string(documentationBytes)
	}
	return documentation, nil
}
