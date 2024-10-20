package drop

import (
	"context"
	"log"
)

type Impl struct {
	*Droppable
	context    context.Context
	cancelRoot context.CancelFunc
}

func NewContext(ctx context.Context) *Impl {
	i := &Impl{}
	i.context, i.cancelRoot = context.WithCancel(ctx)
	i.Droppable = &Droppable{}
	return i
}

func (s *Impl) Context() context.Context {
	return s.context
}

// Shutdown method of implementing the cleaning of resources and connections when the application is shut down.
// Shutdown method addresses all connected listeners (which implement the corresponding interface)
// and calls the methods of this interface in turn.
// Shutdown accepts a single argument - a callback function with an argument of type error for custom error handling.
// ```code
//
//	Shutdown(func(err error) {
//		log.Warning(err)
//		bugsnag.Notify(err)
//	})
//
// ```
func (s *Impl) Shutdown(onError func(error)) {
	s.cancelRoot()
	log.Println("root context is canceled")

	s.EachDroppers(func(dropper Drop) {
		if impl, ok := dropper.(Debug); ok {
			log.Println(impl.DropMsg())
		}
		if err := dropper.Drop(); err != nil {
			onError(err)
		}
	})
}
