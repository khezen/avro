#!/bin/bash

unit_tests(){
    local ret
    ret=0
    for d in $(go list ./...); do 
        COV=$(go test -race  -coverprofile=profile.out -covermode=atomic $d | sed 's/.*\[no test files\]/0/g' | sed 's/.*coverage//g' | sed  's/[^0-9.]*//g' | sed  's/\.[0-9.]*//g') 
        if [ -f profile.out ]; then 
            cat profile.out >> coverage.txt;
            rm profile.out;
        fi 
        if test $COV -lt 75; then  
            echo expecting test coverage greater than 75 %, got insufficient $COV % for package $d; 
            ret=1
        fi
    done
    return $ret
}

set -e
unit_tests