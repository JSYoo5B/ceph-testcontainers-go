//go:build all || integration

package integration_test

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// smtpSinkScript is an SMTP receiver for the alerts module, run after
// moduleReceiverPrelude with the port as argv[1]. It answers EHLO, MAIL,
// RCPT, DATA and QUIT, relays nothing, and records each message's peer
// address, envelope and data. While moduleReceiverDir/mode reads deny-auth
// when a connection opens, it advertises AUTH PLAIN LOGIN and refuses every
// AUTH with 535, recording each attempt.
const smtpSinkScript = `PORT = int(sys.argv[1])
listener = None
def mode():
    try:
        with open(BASE + '/mode') as f:
            return f.read().strip()
    except FileNotFoundError:
        return 'accept'
def session(conn, peer):
    deny = mode() == 'deny-auth'
    stream = conn.makefile('rb')
    def reply(text):
        conn.sendall((text + '\r\n').encode())
    reply('220 tc-sink ESMTP')
    sender, recipients = '', []
    while True:
        line = stream.readline()
        if not line:
            return
        command = line.decode('utf-8', 'replace').rstrip('\r\n')
        verb = command.split(' ', 1)[0].upper()
        if verb == 'EHLO' and deny:
            reply('250-tc-sink')
            reply('250 AUTH PLAIN LOGIN')
        elif verb in ('EHLO', 'HELO'):
            reply('250 tc-sink')
        elif verb == 'AUTH':
            record({'event': 'auth', 'peer': peer, 'mechanism': command.split(' ')[1] if ' ' in command else ''})
            reply('535 5.7.8 Authentication credentials invalid')
        elif verb == 'MAIL':
            sender, recipients = command.split(':', 1)[1].strip(), []
            reply('250 OK')
        elif verb == 'RCPT':
            recipients.append(command.split(':', 1)[1].strip())
            reply('250 OK')
        elif verb == 'DATA':
            reply('354 End data with <CR><LF>.<CR><LF>')
            data = []
            while True:
                line = stream.readline()
                if not line:
                    return
                if line in (b'.\r\n', b'.\n'):
                    break
                if line.startswith(b'..'):
                    line = line[1:]
                data.append(line.decode('utf-8', 'replace').replace('\r\n', '\n'))
            record({'event': 'message', 'peer': peer, 'from': sender, 'to': recipients, 'data': ''.join(data)})
            reply('250 OK')
        elif verb in ('RSET', 'NOOP'):
            reply('250 OK')
        elif verb == 'QUIT':
            reply('221 Bye')
            return
        else:
            reply('502 Command not implemented')
def handle(conn, peer):
    try:
        session(conn, peer)
    except OSError:
        pass
    finally:
        conn.close()
def accept(sock):
    while True:
        try:
            conn, peer = sock.accept()
        except OSError:
            return
        threading.Thread(target=handle, args=(conn, peer[0]), daemon=True).start()
def open_listener():
    global listener
    listener = socket.create_server(('', PORT))
    threading.Thread(target=accept, args=(listener,), daemon=True).start()
def close_listener():
    global listener
    if listener is not None:
        try:
            listener.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        listener.close()
        listener = None
serve_receiver()
`

// smtpRecord is one record of smtpSinkScript.
type smtpRecord struct {
	Time      float64  `json:"time"`
	Event     string   `json:"event"`
	Peer      string   `json:"peer"`
	Mechanism string   `json:"mechanism"`
	From      string   `json:"from"`
	To        []string `json:"to"`
	Data      string   `json:"data"`
}

// alertEntry is one health check line of an alerts module message, with its
// detail lines.
type alertEntry struct {
	Severity, Code, Summary string
	Details                 []string
}

// alertMessage is the message the alerts module sends (_send_alert_smtp in
// src/pybind/mgr/alerts/module.py): headers, the overall status, the New,
// Updated and Cleared sections of the change it reports and the full health
// status after it.
type alertMessage struct {
	Headers  map[string]string
	Status   string
	Sections map[string][]alertEntry
}

// The section names of an alerts message.
const (
	alertsNew     = "New"
	alertsUpdated = "Updated"
	alertsCleared = "Cleared"
	alertsFull    = "Full health status"
)

var (
	alertSectionLine = regexp.MustCompile(`^(?:--- (New|Updated|Cleared) ---|=== (Full health status) ===)$`)
	alertEntryLine   = regexp.MustCompile(`^\[([A-Z]+)\] ([A-Z][A-Z0-9_]*): (.*)$`)
)

// parseAlertMessage parses the data of one alerts module message. It
// refuses a line it cannot place, so a format change fails the test instead
// of passing with empty sections.
func parseAlertMessage(data string) (alertMessage, error) {
	headers, body, found := strings.Cut(data, "\n\n")
	if !found {
		return alertMessage{}, fmt.Errorf("no header end in %q", data)
	}
	message := alertMessage{Headers: map[string]string{}, Sections: map[string][]alertEntry{}}
	for line := range strings.SplitSeq(headers, "\n") {
		name, value, ok := strings.Cut(line, ": ")
		if !ok {
			return alertMessage{}, fmt.Errorf("header line %q", line)
		}
		message.Headers[name] = value
	}
	section := ""
	for line := range strings.SplitSeq(strings.TrimRight(body, "\n"), "\n") {
		switch match := alertSectionLine.FindStringSubmatch(line); {
		case line == "":
		case match != nil:
			section = match[1] + match[2]
			if _, repeated := message.Sections[section]; repeated {
				return alertMessage{}, fmt.Errorf("section %s repeated", section)
			}
			message.Sections[section] = []alertEntry{}
		case section == "" && message.Status == "":
			message.Status = line
		case section != "" && strings.HasPrefix(line, "        ") && len(message.Sections[section]) > 0:
			entries := message.Sections[section]
			entries[len(entries)-1].Details = append(entries[len(entries)-1].Details, strings.TrimPrefix(line, "        "))
		case section != "":
			entry := alertEntryLine.FindStringSubmatch(line)
			if entry == nil {
				return alertMessage{}, fmt.Errorf("line %q in section %s", line, section)
			}
			message.Sections[section] = append(message.Sections[section], alertEntry{Severity: entry[1], Code: entry[2], Summary: entry[3]})
		default:
			return alertMessage{}, fmt.Errorf("line %q before the first section", line)
		}
	}
	if message.Headers["Subject"] == "" || message.Status != message.Headers["Subject"] {
		return alertMessage{}, fmt.Errorf("subject %q and status %q differ", message.Headers["Subject"], message.Status)
	}
	if _, ok := message.Sections[alertsFull]; !ok {
		return alertMessage{}, fmt.Errorf("no full health status section")
	}
	return message, nil
}

// Codes returns the sorted health check codes of one section.
func (m alertMessage) Codes(section string) []string {
	var codes []string
	for _, entry := range m.Sections[section] {
		codes = append(codes, entry.Code)
	}
	slices.Sort(codes)
	return codes
}

// Change reports whether the message reports a change: a forced send has no
// New, Updated or Cleared section.
func (m alertMessage) Change() bool {
	for _, section := range []string{alertsNew, alertsUpdated, alertsCleared} {
		if _, ok := m.Sections[section]; ok {
			return true
		}
	}
	return false
}
