#!/bin/sh

find \
	. \
	-type f \
	-name '*.go' |
	xargs \
		gofmt \
		-s \
		-w
