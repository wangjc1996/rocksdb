#! /bin/bash
make uninstall
make clean
make -j8 install-shared
make shared_lib
