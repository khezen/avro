package avro

import "github.com/valyala/fastjson"

func translateValueToMetaFields(value *fastjson.Value) (namespace, name, documentation string, aliases []string, err error) {
	if value.Exists("namespace") {
		namespace = string(value.GetStringBytes("namespace"))
	}
	if value.Exists("name") {
		name = string(value.GetStringBytes("name"))
	}
	if value.Exists("doc") {
		documentation = string(value.GetStringBytes("doc"))
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
