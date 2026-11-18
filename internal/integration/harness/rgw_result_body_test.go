//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package harness

import (
	"strings"
	"testing"
)

func TestRGWResultBodyRequiresCompleteDocument(t *testing.T) {
	for _, root := range []string{"CopyObjectResult", "CompleteMultipartUploadResult"} {
		for _, body := range []string{
			"<" + root + ">",
			"<" + root + "><ETag>x</" + root + ">",
			"<" + root + "></Other>",
			"<" + root + "/>trailing",
			"<" + root + "/><Other/>",
			"<Error><Code>Denied</Code>",
			"<Error><Code>Denied</Code></Error>junk",
			"<Error><Code>Denied</Code></Error><Error/>",
			"junk<" + root + "/>",
		} {
			if result, err := RGWClassifyResultBody([]byte(body), root); err == nil {
				t.Fatalf("accepted %q as %+v", body, result)
			}
		}
		for _, body := range []string{"<" + root + "/>", "<" + root + " xmlns='urn:s3'><ETag>x</ETag></" + root + ">", "<Error xmlns='urn:s3'><Code>Denied</Code></Error>"} {
			result, err := RGWClassifyResultBody([]byte(body), root)
			if err != nil || result.Error != strings.HasPrefix(body, "<Error") || (result.Error && result.Code != "Denied") {
				t.Fatalf("%s: %+v %v", body, result, err)
			}
		}
	}
}
