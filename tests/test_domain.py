"""领域资料及现有接口核对。"""
import json,tempfile,unittest
from pathlib import Path
from warehouse.objects import ObjectStore
from warehouse.cursor import inspect

class DomainTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.store=ObjectStore(self.root/'objects')
        self.data=json.loads(Path('data/source.json').read_text(encoding='utf-8'))
    def test_existing_reader(self):
        result=inspect(self.data,self.store,self.root)
        self.assertTrue(result)

    def test_token_and_query_binding(self):
        from warehouse.cursor import sign,verify,Results,fingerprint
        secret=b'local-test-secret';token=sign({'query':'a','position':0},secret)
        self.assertEqual(verify(token,secret)['position'],0)
        with self.assertRaises(ValueError):verify(token,b'other-secret')
        r=Results(self.store);d=r.put_fragment('a','f',[{'occurrence_id':'x'}])
        with self.assertRaises(ValueError):r.read_fragment(d,'b')
        self.assertNotEqual(fingerprint('a','s',{},[]),fingerprint('b','s',{},[]))
