package command

// normal hashmap is ok since this will only be read
type CommandRegistry map[string]Command

func NewCommandRegistry() CommandRegistry {
	cmdReg := CommandRegistry{
		"get": &get{},
		"set": &set{},
		"delete": &delete{},
	}
	return cmdReg	
}
