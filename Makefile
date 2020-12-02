
default: build

all: default cp

build:
	go build -o bin/testutil main.go

cp:
	cp bin/testutil ~/bin/
