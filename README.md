# Collation Extractor for [go-mysql-server](https://github.com/dolthub/go-mysql-server)

This tool is designed to extract character sets and collations from a MySQL instance, which will then be added to [go-mysql-server](https://github.com/dolthub/go-mysql-server).

## How to Use

The root directory contains multiple test files, each with their own functionality.
To use one of the functions, modify the constant variables within the test file, and run the test.
Each function has a comment further describing its purpose and any additional considerations specific to that function.

It is recommended to read and understand all tests before using any of them, as some concepts are repeated between tests, but may only be explained in one of them.

## Why Test Files?

It's quicker to write them.
Rather than dealing with argument parsing and the like, it's easier to make a new test file with some constant variables.
This is intended for developers of [go-mysql-server](https://github.com/dolthub/go-mysql-server), and the tool should only need to be run when a new character and/or collation is added to MySQL, which is not a common occurrence.
Therefore, developer ergonomics are prioritized over all else.
It is unlikely that this tool will ever be run outside of an IDE.
