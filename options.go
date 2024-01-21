package microbatch

import "time"

type optfunc func(*Batcher)

func BatchSize(size int) optfunc {
	return func(b *Batcher) {
		b.size = size
	}
}

func ProcessFrequency(f time.Duration) optfunc {
	return func(b *Batcher) {
		b.frequency = f
	}
}
