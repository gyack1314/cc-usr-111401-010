"""查询身份与不透明令牌的基础签验。"""
import hashlib,hmac,base64,json
from .objects import encode

def fingerprint(tenant,snapshot,parameters,order):
    return hashlib.sha256(encode({'tenant':tenant,'snapshot':snapshot,'parameters':parameters,'order':order})).hexdigest()

def sign(payload,secret):
    body=encode(payload);signature=hmac.new(secret,body,hashlib.sha256).digest()
    return base64.urlsafe_b64encode(body+signature).decode('ascii')

def verify(token,secret):
    try:raw=base64.b64decode(token,altchars=b'-_',validate=True)
    except Exception:raise ValueError('令牌编码非法') from None
    if len(raw)<33 or not hmac.compare_digest(raw[-32:],hmac.new(secret,raw[:-32],hashlib.sha256).digest()):raise ValueError('令牌签名不符')
    return json.loads(raw[:-32])

from .results import Results

def inspect(data,store,root):
    q=data['queries'][0];identity=fingerprint(q['tenant'],q['snapshot'],q['parameters'],q['order']);r=Results(store)
    fragments=[r.put_fragment(identity,f['fragment_id'],f['rows']) for f in data['fragments']]
    return {'query_id':identity,'fragment_rows':[len(r.read_fragment(f,identity)) for f in fragments],'fragments':fragments}
