package util

import (
	"strconv"
	"strings"
)

func ParseUint64Array(str string) []uint64 {
	nums := make([]uint64, 0)
	if str != "" {
		numsStr := strings.Split(str, ",")
		for _, ns := range numsStr {
			if n, err := strconv.ParseUint(ns, 10, 32); err != nil {
				panic(err)
			} else if n < 0 {
				panic("negative rate is invalid")
			} else {
				nums = append(nums, n)
			}
		}
	}
	return nums
}
