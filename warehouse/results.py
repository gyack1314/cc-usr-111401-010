"""不可变查询结果片段存储。"""
class Results:
    def __init__(self,store):self.store=store
    def put_fragment(self,query_id,fragment_id,rows):
        if len({r['occurrence_id'] for r in rows})!=len(rows):raise ValueError('结果实例身份重复')
        return self.store.put_json({'query_id':query_id,'fragment_id':fragment_id,'rows':rows})
    def read_fragment(self,digest,query_id):
        value=self.store.json(digest)
        if value['query_id']!=query_id:raise ValueError('查询身份不符')
        return value['rows']

