#!/usr/bin/env bash
rm -f source/*
sphinx-apidoc -f -o ./source ../src/hdx
