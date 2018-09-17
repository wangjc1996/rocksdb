#! /bin/bash
make uninstall
make clean
make -j install-shared
make shared_lib
