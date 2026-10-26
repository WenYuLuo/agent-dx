"""Restore two pinned product inputs in the disposable Linux build copy only."""
import base64,hashlib,json,pathlib,sys,zlib
root=pathlib.Path('/workspace');assert pathlib.Path.cwd()==root
allowed={'afs/client/src/connection.rs','afs/src/node/api/usrbio.rs'}
files=json.loads(zlib.decompress(base64.b64decode(sys.argv[1])))
assert set(files)==allowed
for name,content in files.items():
 p=root/name;p.write_bytes(base64.b64decode(content));print(name,hashlib.sha256(p.read_bytes()).hexdigest())
