//go:build all || integration

package integration_test

// radosHeaderWriterScript sets an object's omap header through the rados
// CLI. Whether setomapheader takes --input-file is read from the CLI's own
// usage text, a read-only check, so a failed write is never mistaken for a
// missing option and retried as a second write: the one write's failure
// propagates. Without --input-file the value goes as an argument, which
// cannot hold a NUL byte.
const radosHeaderWriterScript = `def input_file_supported(help_text):
    for line in help_text.splitlines():
        if line.strip().startswith('setomapheader'):
            return '--input-file' in line
    raise AssertionError('rados --help lists no setomapheader')
def set_header(name,data):
    usage=cli('--help',ok=False)
    if input_file_supported((usage.stdout+usage.stderr).decode(errors='replace')):
        with tempfile.TemporaryDirectory() as d:
            path=os.path.join(d,'h')
            with open(path,'wb') as f: f.write(data)
            cli('setomapheader',name,'--input-file',path)
        return 'input-file'
    assert b'\0' not in data
    cli('setomapheader',name,data)
    return 'argument'
`
