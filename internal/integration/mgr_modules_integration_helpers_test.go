//go:build all || (integration && features)

package integration_test

const mgrModuleRBDProbe = `import json,subprocess,sys,time
pool=sys.argv[1]
def run(*args):
    command=["ceph","rbd",*args] if args[0]=="task" else ["rbd",*args]
    result=subprocess.run(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=35)
    assert result.returncode==0, (command,result.returncode,result.stderr)
    return result.stdout
run("mirror","pool","enable",pool,"image")
run("create",pool+"/scheduled","--size","4")
run("mirror","image","enable",pool+"/scheduled","snapshot")
try:
    run("mirror","snapshot","schedule","add","--pool",pool,"1d")
    schedules=json.loads(run("mirror","snapshot","schedule","list","--pool",pool,"--recursive","--format","json"))
    serialized=json.dumps(schedules)
    assert pool in serialized and "1d" in serialized,serialized
    run("create",pool+"/task-remove","--size","4")
    task=json.loads(run("task","add","remove",pool+"/task-remove","--format","json"))
    assert task.get("id") and task["refs"]["action"]=="remove" and task["refs"]["pool_name"]==pool,task
    deadline=time.monotonic()+45
    while "task-remove" in json.loads(run("ls",pool,"--format","json")):
        assert time.monotonic()<deadline,"rbd_support task did not remove its image"
        time.sleep(.5)
    json.loads(run("task","list","--format","json"))
    print("rbd_support: schedule metadata installed; native remove task accepted and completed")
finally:
    run("mirror","snapshot","schedule","remove","--pool",pool,"1d")
    run("mirror","image","disable",pool+"/scheduled")
    run("rm",pool+"/scheduled")
`
