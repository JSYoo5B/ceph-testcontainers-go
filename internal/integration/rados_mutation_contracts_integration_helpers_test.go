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
const radosMutationContractScript = `import ctypes,json,sys,time,rados
phase,pool,namespace=sys.argv[1],sys.argv[2],sys.argv[3]
def connected(timeout='10'):
    c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':timeout,'rados_mon_op_timeout':'10'})
    c.connect(); return c
def opened(c):
    i=c.open_ioctx(pool); i.set_namespace(namespace); return i
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
        size,_=io.stat(name)
    except rados.ObjectNotFound:
        return {'exists':False}
    data=io.read(name,size or 1,0)
    xattr=None
    try:
        xattr=io.get_xattr(name,'tc').decode()
    except rados.NoData:
        pass
    return {'exists':True,'size':size,'data':data.hex(),'xattr':xattr,'version':io.get_last_version()}
def cmpxattr(name,value,data):
    # python-rados has no cmpxattr; call librados directly.
    lib=ctypes.CDLL('librados.so.2')
    cluster=ctypes.c_void_p(); io=ctypes.c_void_p()
    assert lib.rados_create2(ctypes.byref(cluster),b'ceph',b'client.admin',ctypes.c_uint64(0))==0
    try:
        assert lib.rados_conf_read_file(cluster,b'/etc/ceph/ceph.conf')==0
        assert lib.rados_connect(cluster)==0
        assert lib.rados_ioctx_create(cluster,pool.encode(),ctypes.byref(io))==0
        lib.rados_ioctx_set_namespace(io,namespace.encode())
        lib.rados_create_write_op.restype=ctypes.c_void_p
        op=ctypes.c_void_p(lib.rados_create_write_op())
        lib.rados_write_op_cmpxattr(op,b'tc',ctypes.c_uint8(rados.LIBRADOS_CMPXATTR_OP_EQ),value,ctypes.c_size_t(len(value)))
        lib.rados_write_op_write_full(op,data,ctypes.c_size_t(len(data)))
        result=lib.rados_write_op_operate(op,io,name.encode(),None,0)
        lib.rados_release_write_op(op)
        lib.rados_ioctx_destroy(io)
        return result
    finally:
        lib.rados_shutdown(cluster)
clients=[]
try:
    writer,reader=connected(),connected(); clients+=[writer,reader]
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
        timeout,linger=sys.argv[4],float(sys.argv[5])
        late=connected(timeout); clients.append(late)
        l=opened(late)
        started=time.time()
        receipt['truncate']=refused(l,'late-truncate',lambda op: op.truncate(10))
        receipt['remove']=refused(l,'late-remove',lambda op: op.remove())
        receipt['compound']=refused(l,'late-compound',lambda op: (op.cmpext(b'0123456789',0),op.write_full(b'compound'),op.set_xattr('tc',b'v2')))
        receipt['seconds']=time.time()-started
        time.sleep(linger)
    elif phase=='state':
        for name in ('late-truncate','late-remove','late-compound'):
            receipt[name]=view(b,name)
    print(json.dumps(receipt))
finally:
    for c in clients: c.shutdown()
`
