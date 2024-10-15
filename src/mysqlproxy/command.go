package sentinel

type Sql string

func (c *Sql) toString() string {
	return string(*c)
}
