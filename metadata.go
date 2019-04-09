package avro

import "github.com/valyala/fastjson"

func translateValueToMetaFields(value *fastjson.Value) (namespace, name, documentation string, aliases []string, err error) {
	if value.Exists("namespace") {
		namespace = value.Get("namespace").String()
	}
	if value.Exists("name") {
		name = value.Get("name").String()
	}
	if value.Exists("doc") {
		documentation = value.Get("doc").String()
	}
	if value.Exists("aliases") {
		aliasValues, err := value.Get("aliases").Array()
		if err != nil {
			return "", "", "", nil, ErrInvalidSchema
		}
		aliases = make([]string, 0, len(aliasValues))
		for _, aliasValue := range aliasValues {
			aliases = append(aliases, aliasValue.String())
		}
	}
	return namespace, name, documentation, aliases, nil
}
