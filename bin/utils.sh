#!/bin/bash

commandExist() {
  if [[ "$(command -v "$1" >/dev/null; echo $?)" -eq 0 ]];
  then
    return 0
  fi
  return 1
}