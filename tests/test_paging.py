"""可续取结果游标的行为核对。"""
import tempfile
import unittest
from pathlib import Path

from warehouse.cursor import sign
from warehouse.objects import ObjectStore
from warehouse.paging import (CANCELLED, DONE, FAILED, PAGE, PENDING,
                              CursorService, row_key)
from warehouse.snapshot import Snapshots

SECRET = b'paging-test-secret'


def make_rows(spec):
    return [{'occurrence_id': occurrence, 'time': time, 'value': value}
            for occurrence, time, value in spec]


class CursorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = ObjectStore(self.root / 'objects')
        self.service = CursorService(self.root / 'cursors', self.store, SECRET,
                                     page_rows=2, ttl_seconds=100)

    def open(self, tenant='alpha', snapshot='s1', parameters=None, order=None, now=0):
        return self.service.open(tenant, snapshot,
                                 parameters if parameters is not None else {'tool': 'chat'},
                                 order if order is not None else ['time'], now=now)

    def test_pages_preserve_ties_and_duplicate_rows(self):
        token = self.open()
        rows = make_rows([('a', 10, 3), ('b', 10, 3), ('c', 10, 3),
                          ('d', 20, 4), ('e', 20, 4)])
        self.service.feed(token, rows[:2], now=1)
        self.service.feed(token, rows[2:], now=2)
        self.service.finish(token, now=3)
        seen = []
        statuses = []
        while True:
            page = self.service.fetch(token, now=4)
            statuses.append(page['status'])
            if page['status'] != PAGE:
                break
            seen.extend(page['rows'])
            self.service.ack(page['next'], now=4)
            token = page['next']
        self.assertEqual(statuses, [PAGE, PAGE, PAGE, DONE])
        self.assertEqual(seen, rows)
        self.assertEqual([r['occurrence_id'] for r in seen],
                         ['a', 'b', 'c', 'd', 'e'])

    def test_retry_same_token_returns_same_page(self):
        token = self.open()
        self.service.feed(token, make_rows([('a', 10, 3), ('b', 20, 4)]), now=1)
        first = self.service.fetch(token, now=2)
        again = self.service.fetch(token, now=3)
        self.assertEqual(again['rows'], first['rows'])
        self.assertEqual(again['next'], first['next'])
        # 页切出后又落地新片段，重试仍得同一页，不会半新半旧。
        self.service.feed(token, make_rows([('c', 30, 5), ('d', 40, 6)]), now=4)
        retry = self.service.fetch(token, now=5)
        self.assertEqual(retry['rows'], first['rows'])
        self.service.ack(first['next'], now=6)
        second = self.service.fetch(first['next'], now=7)
        self.assertEqual([r['occurrence_id'] for r in second['rows']], ['c', 'd'])

    def test_pending_is_not_the_end(self):
        token = self.open()
        empty = self.service.fetch(token, now=1)
        self.assertEqual(empty['status'], PENDING)
        self.service.feed(token, make_rows([('a', 10, 3)]), now=2)
        page = self.service.fetch(token, now=3)
        self.assertEqual(page['status'], PAGE)
        self.service.ack(page['next'], now=4)
        self.assertEqual(self.service.fetch(page['next'], now=5)['status'], PENDING)
        self.service.finish(token, now=6)
        self.assertEqual(self.service.fetch(page['next'], now=7)['status'], DONE)

    def test_failed_and_cancelled_are_distinct(self):
        token = self.open()
        self.service.feed(token, make_rows([('a', 10, 3)]), now=1)
        self.service.fail(token, '执行器崩溃', now=2)
        failed = self.service.fetch(token, now=3)
        self.assertEqual(failed['status'], FAILED)
        self.assertEqual(failed['reason'], '执行器崩溃')
        other = self.open(tenant='beta')
        self.service.cancel(other, now=4)
        self.assertEqual(self.service.fetch(other, now=5)['status'], CANCELLED)
        for t in (token, other):
            with self.assertRaises(ValueError):
                self.service.land(t, None, make_rows([('z', 99, 9)]), now=6)

    def test_token_binds_identity_and_position(self):
        token = self.open()
        self.service.feed(token, make_rows([('a', 10, 3)]), now=1)
        # 其他密钥无法验签。
        with self.assertRaises(ValueError):
            self.service.fetch(token.replace(token[-1], 'A' if token[-1] != 'A' else 'B'),
                               now=2)
        # 伪造的游标身份与查询身份组合被拒绝。
        state = self.service.describe(token, now=2)
        forged = sign({'cursor': state['cursor'], 'query': 'other-identity',
                       'position': 0}, SECRET)
        with self.assertRaises(ValueError):
            self.service.fetch(forged, now=2)
        # 越过未确认页的位置被拒绝。
        ahead = sign({'cursor': state['cursor'], 'query': state['identity'],
                      'position': 99}, SECRET)
        with self.assertRaises(ValueError):
            self.service.fetch(ahead, now=2)
        # 另一组参数是另一个游标，原令牌不能续取它。
        other = self.open(parameters={'tool': 'search'})
        self.assertNotEqual(self.service.describe(token, now=2)['identity'],
                            self.service.describe(other, now=2)['identity'])

    def test_cross_process_resume(self):
        token = self.open()
        self.service.feed(token, make_rows([('a', 10, 3), ('b', 20, 4),
                                            ('c', 30, 5)]), now=1)
        self.service.finish(token, now=2)
        first = self.service.fetch(token, now=3)
        self.service.ack(first['next'], now=4)
        # 用同一目录与密钥重建服务，模拟另一进程恢复。
        revived = CursorService(self.root / 'cursors', self.store, SECRET,
                                page_rows=2, ttl_seconds=100)
        second = revived.fetch(first['next'], now=5)
        self.assertEqual([r['occurrence_id'] for r in second['rows']], ['c'])
        self.assertEqual(revived.ack(second['next'], now=6)['confirmed'], 3)
        self.assertEqual(revived.fetch(second['next'], now=7)['status'], DONE)

    def test_expiry_and_sweep_reclaim_objects(self):
        token = self.open(now=0)
        landed = self.service.land(token, None, make_rows([('a', 10, 3)]), now=1)
        digest = landed['digest']
        self.assertTrue(self.store.exists(digest))
        with self.assertRaises(ValueError):
            self.service.fetch(token, now=200)
        reclaimed = self.service.sweep(now=200)
        self.assertFalse(self.store.exists(digest))
        self.assertEqual(len(reclaimed['cursors']), 1)
        with self.assertRaises(ValueError):
            self.service.fetch(token, now=201)

    def test_cancelled_objects_reclaimed_but_status_visible(self):
        token = self.open(now=0)
        digest = self.service.land(token, None, make_rows([('a', 10, 3)]), now=1)['digest']
        self.service.cancel(token, now=2)
        reclaimed = self.service.sweep(now=3)
        self.assertEqual(reclaimed['objects'], 1)
        self.assertFalse(self.store.exists(digest))
        self.assertEqual(self.service.fetch(token, now=4)['status'], CANCELLED)
        self.service.sweep(now=500)
        with self.assertRaises(ValueError):
            self.service.fetch(token, now=501)

    def test_ack_advances_reclamation_frontier(self):
        token = self.open()
        digests = []
        for batch in ([('a', 10, 3), ('b', 20, 4)], [('c', 30, 5), ('d', 40, 6)]):
            digests.append(self.service.land(
                token, None, make_rows(batch), now=1)['digest'])
        self.service.finish(token, now=2)
        before = self.service.describe(token, now=3)['buffered_bytes']
        first = self.service.fetch(token, now=3)
        self.service.ack(first['next'], now=4)
        # 第一页确认后，完全落在确认位置之前的片段立即回收。
        self.assertFalse(self.store.exists(digests[0]))
        self.assertTrue(self.store.exists(digests[1]))
        after = self.service.describe(token, now=5)['buffered_bytes']
        self.assertGreater(before, after)
        self.assertGreater(after, 0)
        second = self.service.fetch(first['next'], now=6)
        self.assertEqual([r['occurrence_id'] for r in second['rows']], ['c', 'd'])

    def test_ack_rules(self):
        token = self.open()
        self.service.feed(token, make_rows([('a', 10, 3), ('b', 20, 4)]), now=1)
        page = self.service.fetch(token, now=2)
        self.assertEqual(self.service.ack(page['next'], now=3)['confirmed'], 2)
        # 重复确认幂等。
        self.assertEqual(self.service.ack(page['next'], now=4)['confirmed'], 2)
        # 与未决页不符的确认位置被拒绝。
        state = self.service.describe(token, now=5)
        wrong = sign({'cursor': state['cursor'], 'query': state['identity'],
                      'position': 1}, SECRET)
        with self.assertRaises(ValueError):
            self.service.ack(wrong, now=6)

    def test_resource_limits(self):
        limited = CursorService(self.root / 'limited', self.store, SECRET,
                                page_rows=2, max_fragment_rows=3,
                                max_buffered_rows=5, ttl_seconds=100)
        token = limited.open('alpha', 's1', {}, ['time'], now=0)
        with self.assertRaises(ValueError):
            limited.land(token, None, make_rows([('a', 1, 1), ('b', 2, 2),
                                                 ('c', 3, 3), ('d', 4, 4)]), now=1)
        limited.land(token, None, make_rows([('a', 1, 1), ('b', 2, 2), ('c', 3, 3)]),
                     now=2)
        with self.assertRaises(ValueError):
            limited.land(token, None, make_rows([('d', 4, 4), ('e', 5, 5), ('f', 6, 6)]),
                         now=3)
        # 超出暂存限制的游标转为失败，不再产出页。
        self.assertEqual(limited.fetch(token, now=4)['status'], FAILED)
        tight = CursorService(self.root / 'tight', self.store, SECRET,
                              max_cursors=1, ttl_seconds=100)
        tight.open('alpha', 's1', {}, ['time'], now=0)
        with self.assertRaises(ValueError):
            tight.open('beta', 's1', {}, ['time'], now=0)

    def test_buffered_bytes_limit(self):
        limited = CursorService(self.root / 'bytes', self.store, SECRET,
                                max_buffered_bytes=200, ttl_seconds=100)
        token = limited.open('alpha', 's1', {}, ['time'], now=0)
        limited.land(token, None, make_rows([('a', 1, 1)]), now=1)
        with self.assertRaises(ValueError):
            limited.land(token, None,
                         make_rows([('b', 2, 10**30), ('c', 3, 10**30),
                                    ('d', 4, 10**30), ('e', 5, 10**30)]), now=2)
        self.assertEqual(limited.fetch(token, now=3)['status'], FAILED)

    def test_order_is_enforced(self):
        token = self.open()
        with self.assertRaises(ValueError):
            self.service.land(token, None, make_rows([('b', 20, 4), ('a', 10, 3)]),
                              now=1)
        self.service.land(token, None, make_rows([('a', 10, 3), ('b', 20, 4)]), now=2)
        # 后续片段的键不能早于已落地结果。
        with self.assertRaises(ValueError):
            self.service.land(token, None, make_rows([('x', 15, 9)]), now=3)
        # 重复的实例身份跨片段同样被拒绝。
        with self.assertRaises(ValueError):
            self.service.land(token, None, make_rows([('b', 20, 4)]), now=4)
        # 排序键并列但实例身份不同则合法。
        self.service.land(token, None, make_rows([('c', 20, 4), ('d', 20, 4)]), now=5)

    def test_snapshot_frozen_for_cursor(self):
        snapshots = Snapshots(self.store)
        rows = make_rows([('b', 20, 4), ('a', 10, 3)])
        for row in rows:
            row['row_id'] = row['occurrence_id']
        first = snapshots.create(rows, {'time': 'int'})
        token = self.service.open('alpha', first, {}, ['time'], now=0)
        # 游标打开后新入仓的资料进入新快照。
        later = snapshots.create(rows + [{'row_id': 'c', 'occurrence_id': 'c',
                                          'time': 5, 'value': 9}], {'time': 'int'})
        self.assertNotEqual(first, later)
        self.service.run_snapshot(token, snapshots, now=1)
        seen = []
        current = token
        while True:
            page = self.service.fetch(current, now=2)
            if page['status'] != PAGE:
                break
            seen.extend(page['rows'])
            self.service.ack(page['next'], now=2)
            current = page['next']
        self.assertEqual([r['occurrence_id'] for r in seen], ['a', 'b'])

    def test_describe_reports_progress(self):
        token = self.open(now=0)
        self.service.feed(token, make_rows([('a', 10, 3), ('b', 20, 4), ('c', 30, 5)]),
                          now=1)
        page = self.service.fetch(token, now=2)
        self.service.ack(page['next'], now=3)
        state = self.service.describe(token, now=4)
        self.assertEqual(state['status'], 'running')
        self.assertEqual(state['confirmed'], 2)
        self.assertEqual(state['total_rows'], 3)
        self.assertGreater(state['buffered_bytes'], 0)

    def test_open_is_idempotent_with_same_cursor_id(self):
        token = self.service.open('alpha', 's1', {}, ['time'], now=0, cursor_id='fixed')
        self.service.feed(token, make_rows([('a', 10, 3)]), now=1)
        again = self.service.open('alpha', 's1', {}, ['time'], now=2, cursor_id='fixed')
        self.assertEqual(self.service.describe(again, now=3)['total_rows'], 1)
        with self.assertRaises(ValueError):
            self.service.open('beta', 's1', {}, ['time'], now=4, cursor_id='fixed')


if __name__ == '__main__':
    unittest.main()
