#!/bin/bash
for i in {1..100}
  do
    cargo run --bin mini-redis-cli set foo$i bar$i
  done