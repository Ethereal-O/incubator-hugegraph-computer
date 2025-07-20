package schedules

type Algorithm interface {
	// Name returns the name of the algorithm
	Name() string
	// Execute runs the algorithm with the provided parameters
	Execute(params map[string]interface{}) (interface{}, error)
	// Validate checks if the provided parameters are valid for the algorithm
}

type AlgorithmManager struct {
	supportedAlgorithms map[string]Algorithm
}

func (am *AlgorithmManager) Init() {
	am.supportedAlgorithms = make(map[string]Algorithm)
}
