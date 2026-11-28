package netem

// Script reports, as one JSON object, what a helper sees. "preflight"
// checks the contract and netem on the loopback of the helper's own
// namespace, with "rate" also netem's rate, with "reorder", "duplicate" or
// "corrupt" those packet faults with a correlation, and with "peers" the prio
// qdisc and u32 classifier, its filter also with a TCP source port key with
// "sport", and with a destination port and a SYN key with "setup"; "inspect
// ADDRESS MAC" finds the interface with that address and MAC in the namespace
// the helper joined, and "index N" the interface with that index; both read
// its qdiscs and egress filters. tc prints each u32 key as another "match"
// member of one object, so the script collects them into a "matches" list
// before they reach a JSON decoder that would keep only the last.
const Script = `import json,os,shutil,subprocess,sys
def pairs(items):
 d={}
 for k,v in items:
  if k=="match": d.setdefault("matches",[]).append(v)
  else: d[k]=v
 return d
def run(*argv):
 p=subprocess.run(argv,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
 return p.returncode,p.stdout.decode(errors="replace"),p.stderr.decode(errors="replace").strip()[-400:]
def net_admin():
 with open("/proc/self/status") as f:
  for line in f:
   if line.startswith("CapEff:"): return bool(int(line.split()[1],16)>>12&1)
 return False
out={"namespace":os.readlink("/proc/self/ns/net"),"net_admin":net_admin()}
if sys.argv[1]=="preflight":
 out["missing"]=[n for n in ("sh","sleep","tc","ip") if shutil.which(n) is None]
 if not out["missing"]:
  broken=[]
  for tool in ("tc","ip"):
   code,o,e=run(tool,"-V")
   out[tool]=(o+e).strip()[:200]
   if code or "iproute2" not in o+e: broken.append(tool+" -V")
  for argv in (("tc","-j","-s","qdisc","show","dev","lo"),("ip","-j","address","show")):
   code,o,e=run(*argv)
   try: json.loads(o)
   except ValueError: broken.append(" ".join(argv))
  out["broken"]=broken
  if not broken:
   out["add"]=run("tc","qdisc","add","dev","lo","root","handle","c7e1:","netem","delay","1ms")[0::2]
   if out["add"][0]==0:
    out["shown"]=run("tc","-j","qdisc","show","dev","lo")[1]
    out["delete"]=run("tc","qdisc","del","dev","lo","root","handle","c7e1:")[0::2]
    if "rate" in sys.argv[2:] and out["delete"][0]==0:
     out["rate"]=run("tc","qdisc","add","dev","lo","root","handle","c7e3:","netem","rate","1000000bit","limit","100")[0::2]
     if out["rate"][0]==0:
      out["rate_shown"]=run("tc","-j","qdisc","show","dev","lo")[1]
      out["rate_delete"]=run("tc","qdisc","del","dev","lo","root","handle","c7e3:")[0::2]
    faults=[f for f in ("reorder","duplicate","corrupt") if f in sys.argv[2:]]
    if faults and out["delete"][0]==0:
     argv=["tc","qdisc","add","dev","lo","root","handle","c7e4:","netem"]
     if "reorder" in faults or "duplicate" in faults: argv+=["delay","1ms" if "reorder" in faults else "0us","0us","0%"]
     for f,p,c in (("reorder","25%","50%"),("duplicate","3%","10%"),("corrupt","2%","5%")):
      if f in faults: argv+=[f,p,c]
     out["faults"]=run(*argv)[0::2]
     if out["faults"][0]==0:
      out["faults_shown"]=run("tc","-j","qdisc","show","dev","lo")[1]
      out["faults_delete"]=run("tc","qdisc","del","dev","lo","root","handle","c7e4:")[0::2]
    if "peers" in sys.argv[2:] and out["delete"][0]==0:
     out["prio"]=run("tc","qdisc","add","dev","lo","root","handle","c7e2:","prio","bands","3","priomap",*["0"]*16)[0::2]
     if out["prio"][0]==0:
      keys=["match","ip","dst","127.0.0.2/32"]
      if "sport" in sys.argv[2:] or "setup" in sys.argv[2:]: keys+=["match","ip","protocol","6","0xff","match","ip","sport" if "sport" in sys.argv[2:] else "dport","7000","0xffff"]
      if "setup" in sys.argv[2:]: keys+=["match","u8","0x02","0x02","at","33"]
      out["u32"]=run("tc","filter","add","dev","lo","parent","c7e2:","protocol","ip","pref","10","handle","800::1","u32",*keys,"flowid","c7e2:3")[0::2]
      if out["u32"][0]==0:
       o=run("tc","-j","filter","show","dev","lo")[1]
       try: out["u32_shown"]=json.dumps(json.loads(o,object_pairs_hook=pairs))
       except ValueError: out["u32_shown"]=o
      out["prio_delete"]=run("tc","qdisc","del","dev","lo","root","handle","c7e2:")[0::2]
else:
 code,o,e=run("ip","-j","address","show")
 if code: out["error"]="ip: "+e
 else:
  links=json.loads(o)
  if sys.argv[1]=="inspect":
   address,mac=sys.argv[2],sys.argv[3]
   links=[l for l in links if l.get("address")==mac and any(a.get("local")==address for a in l.get("addr_info",[]))]
  else:
   links=[l for l in links if l.get("ifindex")==int(sys.argv[2])]
  out["interfaces"]=[{"name":l["ifname"],"index":l["ifindex"]} for l in links]
  if len(links)==1:
   code,o,e=run("tc","-j","-s","qdisc","show","dev",links[0]["ifname"])
   if code: out["error"]="tc: "+e
   else:
    out["qdiscs"]=json.loads(o)
    code,o,e=run("tc","-j","filter","show","dev",links[0]["ifname"])
    if code: out["error"]="tc filter: "+e
    else: out["filters"]=json.loads(o,object_pairs_hook=pairs) if o.strip() else []
print(json.dumps(out))
`
