//go:build all || (integration && features)

package integration_test

// radosMutationContractScript runs one phase of the mutation contracts
// against pool and namespace and prints a JSON receipt. Every result is read
// back by a second client.
//   - contracts: zero, exclusive and idempotent create, and the refusals of
//     cmpext, omap_cmp and cmpxattr guards in compound operations.
//   - prepare: the objects for the late phase.
//   - late: a truncate, a remove and a guarded compound write whose client
//     gives up after timeout seconds while the OSD still holds them.
//   - state: the objects of the late phase.
const radosMutationContractScript = radosMutationDiagnosticsScript + `import ctypes,json,sys,time,rados
phase,pool,namespace=sys.argv[1],sys.argv[2],sys.argv[3]
if phase=='late': diag_start(phase,pool,namespace,sys.argv[6],sys.argv[7])
def connected(timeout='10',label='client'):
    c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':timeout,'rados_mon_op_timeout':'10'})
    c.connect();diag_connected(c,label,timeout,'10');return c
def refused(io,name,build):
    try:
        with rados.WriteOpCtx() as op:
            build(op)
            io.operate_write_op(op,name)
    except rados.Error as e:
        return getattr(e,'errno',None)
    return 0
def view(io,name):
    try:
        size,_=diag_call(io,name,'stat',lambda:io.stat(name))
    except rados.ObjectNotFound:
        return {'exists':False}
    data=diag_call(io,name,'read',lambda:io.read(name,size or 1,0))
    xattr=None
    try:
        xattr=diag_call(io,name,'get_xattr',lambda:io.get_xattr(name,'tc')).decode()
    except rados.NoData:
        pass
    return {'exists':True,'size':size,'data':data.hex(),'xattr':xattr,'version':diag_call(io,name,'get_last_version',lambda:io.get_last_version())}
def cmpxattr(name,value,data):
    # python-rados has no cmpxattr; call librados directly.
    lib=ctypes.CDLL('librados.so.2')
    lib.rados_create_write_op.restype=ctypes.c_void_p
    cluster=ctypes.c_void_p(); io=ctypes.c_void_p(); op=None
    assert lib.rados_create2(ctypes.byref(cluster),b'ceph',b'client.admin',ctypes.c_uint64(0))==0
    try:
        assert lib.rados_conf_read_file(cluster,b'/etc/ceph/ceph.conf')==0
        assert lib.rados_connect(cluster)==0
        assert lib.rados_ioctx_create(cluster,pool.encode(),ctypes.byref(io))==0
        lib.rados_ioctx_set_namespace(io,namespace.encode())
        op=ctypes.c_void_p(lib.rados_create_write_op())
        lib.rados_write_op_cmpxattr(op,b'tc',ctypes.c_uint8(rados.LIBRADOS_CMPXATTR_OP_EQ),value,ctypes.c_size_t(len(value)))
        lib.rados_write_op_write_full(op,data,ctypes.c_size_t(len(data)))
        return lib.rados_write_op_operate(op,io,name.encode(),None,0)
    finally:
        if op is not None:
            lib.rados_release_write_op(op)
        if io.value:
            lib.rados_ioctx_destroy(io)
        lib.rados_shutdown(cluster)
clients=[]; ioctxs=[]
def opened(c):
    i=c.open_ioctx(pool); i.set_namespace(namespace);ioctxs.append(i);diag_opened(c,i);return i
try:
    writer,reader=connected('10','writer'),connected('10','reader');clients+=[writer,reader]
    a,b=opened(writer),opened(reader)
    receipt={}
    if phase=='contracts':
        pattern=bytes(range(256))*32
        a.write_full('zeroed',pattern)
        with rados.WriteOpCtx() as op:
            op.zero(1024,2048)
            a.operate_write_op(op,'zeroed')
        receipt['zeroed']=view(b,'zeroed')
        receipt['zeroed_expected']=(pattern[:1024]+bytes(2048)+pattern[3072:]).hex()

        receipt['create_exclusive']=refused(a,'created',lambda op: op.new(rados.LIBRADOS_CREATE_EXCLUSIVE))
        receipt['create_exclusive_again']=refused(a,'created',lambda op: op.new(rados.LIBRADOS_CREATE_EXCLUSIVE))
        receipt['create_idempotent']=refused(a,'created',lambda op: op.new(rados.LIBRADOS_CREATE_IDEMPOTENT))
        receipt['create_exclusive_write']=refused(a,'created',lambda op: (op.new(rados.LIBRADOS_CREATE_EXCLUSIVE),op.write_full(b'replaced')))
        receipt['created']=view(b,'created')

        a.write_full('compared',b'abcdefgh')
        a.set_xattr('compared','tc',b'v1')
        receipt['cmpext_mismatch']=refused(a,'compared',lambda op: (op.cmpext(b'abcXefgh',0),op.write_full(b'replaced'),op.set_xattr('tc',b'v2')))
        receipt['after_cmpext_mismatch']=view(b,'compared')
        with rados.WriteOpCtx() as op:
            a.set_omap(op,('k',),(b'v1',))
            a.operate_write_op(op,'compared')
        receipt['omap_mismatch']=refused(a,'compared',lambda op: (op.omap_cmp('k','v2',rados.LIBRADOS_CMPXATTR_OP_EQ),op.write_full(b'replaced')))
        receipt['after_omap_mismatch']=view(b,'compared')
        receipt['cmpxattr_mismatch']=cmpxattr('compared',b'v2',b'replaced')
        receipt['after_cmpxattr_mismatch']=view(b,'compared')
        receipt['cmpxattr_match']=cmpxattr('compared',b'v1',b'replaced')
        receipt['after_cmpxattr_match']=view(b,'compared')
        receipt['max_errno']=rados.MAX_ERRNO
    elif phase=='prepare':
        for name in ('late-truncate','late-remove','late-compound'):
            a.write_full(name,b'0123456789'*100)
            a.set_xattr(name,'tc',b'v1')
            receipt[name]=view(b,name)
    elif phase=='late':
        timeout,bound=sys.argv[4],float(sys.argv[5])
        late=connected(timeout,'late-writer');clients.append(late)
        l=opened(late)
        receipt['client']='client.%d'%late.get_instance_id()
        receipt['completed']=[]
        for key,name,build in (('truncate','late-truncate',lambda op: op.truncate(10)),
                               ('remove','late-remove',lambda op: op.remove()),
                               ('compound','late-compound',lambda op: (op.cmpext(b'0123456789',0),op.write_full(b'compound'),op.set_xattr('tc',b'v2')))):
            diag_event(l,name,key,'started')
            receipt[key]=refused(l,name,build)
            receipt['completed'].append(time.time())
            diag_mutation(l,name,key,receipt[key],receipt['completed'][-1])
        # Stay connected until the OSD has applied all three: it discards an
        # operation whose connection closed before it started.
        diag_stage('observing')
        deadline=time.monotonic()+bound
        while True:
            truncated,removed,compound=view(b,'late-truncate'),view(b,'late-remove'),view(b,'late-compound')
            if truncated.get('size')==10 and not removed['exists'] and compound.get('xattr')=='v2':
                break
            assert time.monotonic()<deadline,'the OSD did not apply the late operations'
            time.sleep(0.5)
    elif phase=='state':
        for name in ('late-truncate','late-remove','late-compound'):
            receipt[name]=view(b,name)
    diag_stage('complete')
    print(json.dumps(receipt))
finally:
    if _diag is None:
        for i in ioctxs: i.close()
        for c in clients: c.shutdown()
    else:
        primary=sys.exc_info()[1]
        cleanup_error=None
        for kind,items,method in (('ioctx',ioctxs,'close'),('client',clients,'shutdown')):
            for item in items:
                try: getattr(item,method)()
                except Exception as error:
                    diag_cleanup(kind,error)
                    if cleanup_error is None: cleanup_error=error
        if primary is None and cleanup_error is not None: raise cleanup_error
`
