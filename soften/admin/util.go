package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"

	"github.com/shenqianjin/soften-client-go/soften/config"
)

func callWithRet(c *http.Client, req *http.Request, ret interface{}) error {
	if config.DebugMode {
		reqBytes, dumpErr := httputil.DumpRequestOut(req, true)
		fmt.Println(string(reqBytes), dumpErr)
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	if config.DebugMode {
		respBytes, dumpErr := httputil.DumpResponse(resp, resp.ContentLength > 0)
		fmt.Println(string(respBytes), dumpErr)
	}
	// success
	if resp.StatusCode/100 == 2 {
		if ret != nil {
			if err1 := json.NewDecoder(resp.Body).Decode(ret); err1 != nil {
				return err1
			}
		}
		return nil
	}
	// failed
	if resp.ContentLength != 0 {
		if respData, err1 := io.ReadAll(resp.Body); err1 != nil {
			err = err1
		} else {
			err = errors.New(fmt.Sprintf("%s => %s", resp.Status, string(respData)))
		}
	} else {
		err = errors.New(fmt.Sprintf("%s", resp.Status))
	}
	return err
}
