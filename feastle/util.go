package feastle

type FeastFeature struct {
	EntityId       string
	EventTimestamp string
	Values         map[string][]byte
}

func GenerateRandomFeature() *FeastFeature {
	randId := "todo"
	randTs := "todo"
	randValue1 := "todo"
	randValue2 := "todo"
	randValue3 := "todo"

	return &FeastFeature{
		EntityId:       randId,
		EventTimestamp: randTs,
		Values: map[string][]byte{
			"key1": []byte(randValue1),
			"key2": []byte(randValue2),
			"key3": []byte(randValue3),
		},
	}
}
