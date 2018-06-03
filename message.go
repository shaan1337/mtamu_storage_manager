package mta

//Message message
type Message struct {
	msg    string
	params map[string]interface{}
	out    chan<- *Message
}
