//go:build all || integration

package harness

import "testing"

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
			"junk<" + root + "/>",
		} {
			if result, err := RGWClassifyResultBody([]byte(body), root); err == nil {
				t.Fatalf("accepted %q as %+v", body, result)
			}
		}
		for _, body := range []string{"<" + root + "/>", "<" + root + " xmlns='urn:s3'><ETag>x</ETag></" + root + ">", "<Error xmlns='urn:s3'><Code>Denied</Code></Error>"} {
			result, err := RGWClassifyResultBody([]byte(body), root)
			if err != nil || result.Error != (result.Code == "Denied") {
				t.Fatalf("%s: %+v %v", body, result, err)
			}
		}
	}
}
