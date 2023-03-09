package docgenerator

type DocumentType string

type Generator struct {
	Itr           int
	Start         int
	End           int
	DocType       DocumentType
	KeySize       int
	DocSize       int
	RandomDocSize bool
	RandomKeySize bool
	Template      interface{}
}

func (g *Generator) HasNext() bool {
	return true
}

func (g *Generator) Next() {

}

func (g *Generator) NextKey() {

}
