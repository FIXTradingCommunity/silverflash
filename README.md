# silverflash

[![Build Status](https://travis-ci.org/FIXTradingCommunity/silverflash.svg?branch=master)](https://travis-ci.org/FIXTradingCommunity/silverflash)

This project is a reference implementation of FIXP Performance Session Layer. It is intended to 
aid developers of financial and other critical applications. Since the protocol stack is 
standardized, it serves to exchange messages between organizations while keeping low latency
characteristics.

## Protocol stack
FIX Performance Session Layer (FIXP) is a lightweight protocol designed to replace the traditional
FIX session protocol (known officially as FIXT) for high performance use cases. It supports both 
point-to-point exchange of application messages as well as multicasts for market data and the like.

FIXP is part of a family of protocols created by the High Performance Working Group
of the FIX Trading Community. FIXP is a session layer protocol (OSI layer 5). 
See specifications at [FIXTradingCommunity/fixp-specification](https://github.com/FIXTradingCommunity/fixp-specification). *This implementation is intended to ease development but it is non-normative for the FIXP protocol. The specification is the ultimate authority.*

At presentation layer (layer 6), this implementation uses Simple Binary Encoding, a high-performance binary encoding of FIX. 
See specifications at [FIXTradingCommunity/fix-simple-binary-encoding](https://github.com/FIXTradingCommunity/fix-simple-binary-encoding).

At transport layer (layer 4), this project provides wrappers for common transports, including TCP 
and UDP, as well as interprocess communications for internal messaging.

## License
© Copyright 2015-2016 FIX Protocol Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Prerequisites
This project requires Java 8. It should run on any platform for which the JVM is supported.

This project is intended to otherwise require minimal dependencies.

## Build
The project is built with Maven. For developer details, see the project wiki.

