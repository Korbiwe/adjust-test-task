package main

import "fmt"

type Ratable interface {
	GetRating() float64
	Pretty() string
}

type Rating struct {
	slice []Ratable
	size  int
}

func NewRating(size int) *Rating {
	return &Rating{
		slice: make([]Ratable, 0, size),
		size:  size,
	}
}

func (r *Rating) TryPush(item Ratable) bool {
	if len(r.slice) == 0 {
		r.slice = append(r.slice, item)
		return true
	}

	for i, value := range r.slice {
		if item.GetRating() > value.GetRating() {
			r.slice = insert(r.slice, i, item)
			if len(r.slice) > r.size {
				r.slice = r.slice[:r.size]
			}
			return true
		}
	}

	return false
}

func (r *Rating) Pretty() string {
	pretty := ""

	for i, value := range r.slice {
		pretty += fmt.Sprintf("%d (Rating: %f): %s\n", i+1, value.GetRating(), value.Pretty())
	}

	return pretty
}

func insert(a []Ratable, index int, value Ratable) []Ratable {
	if len(a) == index {
		return append(a, value)
	}

	a = append(a[:index+1], a[index:]...)

	a[index] = value
	return a
}
