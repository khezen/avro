package sqlavro

// Query -
func Query(cfg QueryConfig) (avroBytes []byte, newCriteria []Criterion, err error) {
	err = cfg.Verify()
	if err != nil {
		return nil, nil, err
	}
	return query2Avro(cfg)
}
