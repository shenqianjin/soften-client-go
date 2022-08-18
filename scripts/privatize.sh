#!/bin/bash

# source env
if [ -z "$SOURCE_DIR" ]; then
    SOURCE_DIR="."
fi
SOURCE_PACKAGE="github.com/shenqianjin/soften-client-go"

# target env
if [ -z "$TARGET_DIR" ]; then
    TARGET_DIR="$HOME/qbox/kodo/libs/soften-client-go"
fi
if [ -z "$TARGET_PACKAGE" ]; then
    # default privatize package
    TARGET_PACKAGE="github.com/qiniu/soften-client-go"
fi

privatize_dirs="soften test examples go.* README.md docs"

# process rollback
if [[ $1 == "rollback" ]]; then
    # shellcheck disable=SC2086
    # shellcheck disable=SC2046
    privatize_files=$(grep $TARGET_PACKAGE -rl $privatize_dirs)
    [ -z "$privatize_files" ] && echo "privatize rollback is already up to date" && exit 0
    # shellcheck disable=SC2086
    sed -i "" "s#$TARGET_PACKAGE#$SOURCE_PACKAGE#g" $privatize_files
else
    # shellcheck disable=SC2046
    # shellcheck disable=SC2086
    privatize_files=$(grep $SOURCE_PACKAGE -rl $privatize_dirs)
    [ -z "$privatize_files" ] && echo "privatize is already up to date" && exit 0
    # shellcheck disable=SC2086
    sed -i "" "s#$SOURCE_PACKAGE#$TARGET_PACKAGE#g" $privatize_files
    for dir in $privatize_dirs; do
        cp -r $SOURCE_DIR/"$dir" "$TARGET_DIR"
    done
fi
exit 0


