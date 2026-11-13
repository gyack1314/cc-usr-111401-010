"""可持久续取的分页结果游标服务。

设计要点：
- 打开游标时冻结租户、快照、参数与排序规则，指纹即查询身份；期间新入仓的资料
  只会落在更新的快照里，不会进入本游标的结果。
- 结果按物理全序（排序键并列由 occurrence_id 打破）追加式落地为不可变片段，
  位置偏移因此永久稳定：同一页令牌重试必得同一页，新片段不会插进旧结果。
- 页令牌经 HMAC 签名，绑定游标身份与已确认位置；取页与确认分离，客户端在
  确认前断线可用同一令牌重取同一页。
- 取页状态区分 page / pending（尚无下一页）/ done（已经结束）/ failed /
  cancelled，暂时的空窗不会被当成终点。
- 每游标 flock 串行化取页、确认与清理；确认推进即回收前沿，过期或取消的
  游标由 sweep 按崩溃安全顺序回收对象，合法读取不会拿到半新半旧的页。
"""
import fcntl
import json
import os
import tempfile
import uuid
from contextlib import contextmanager
from pathlib import Path

from .cursor import fingerprint, sign, verify
from .objects import encode
from .results import Results

# 查询执行状态（契约规定的终态）
RUNNING = 'running'
COMPLETE = 'complete'
FAILED = 'failed'
CANCELLED = 'cancelled'
# 取页结果状态
PAGE = 'page'
PENDING = 'pending'
DONE = 'done'


def _comparable(value):
    """把键值映射为可全序比较且保留类型差异的元组。"""
    if value is None:
        return (0,)
    if isinstance(value, bool):
        return (1, value)
    if isinstance(value, int):
        return (2, value)
    if isinstance(value, str):
        return (3, value)
    if isinstance(value, (list, tuple)):
        return (4, tuple(_comparable(item) for item in value))
    raise ValueError('键类型不支持')


def key_columns(order):
    """物理全序的列：排序规则加上 occurrence_id 打破并列。"""
    columns = list(order)
    if 'occurrence_id' not in columns:
        columns.append('occurrence_id')
    return columns


def row_key(order, row):
    """结果行的全序键；缺排序列或实例身份即拒绝。"""
    if not isinstance(row, dict):
        raise ValueError('结果行必须是对象')
    if 'occurrence_id' not in row:
        raise ValueError('行缺少结果实例身份')
    key = []
    for column in key_columns(order):
        if column not in row:
            raise ValueError('行缺少排序键')
        key.append(_comparable(row[column]))
    return tuple(key)


class CursorService:
    """跨进程可恢复的结果游标服务。

    所有状态落在 root 目录（每游标一个状态文件），对象保存在传入的
    ObjectStore 中；用同一 root、store 与 secret 重建服务即可继续既有游标。
    时间一律由调用方以 now 显式传入（逻辑时钟）。
    """

    def __init__(self, root, store, secret, *, page_rows=1000, max_fragment_rows=10000,
                 max_buffered_rows=1000000, max_buffered_bytes=268435456,
                 ttl_seconds=3600, max_cursors=1024):
        for name, value in (('page_rows', page_rows), ('max_fragment_rows', max_fragment_rows),
                            ('max_buffered_rows', max_buffered_rows),
                            ('max_buffered_bytes', max_buffered_bytes),
                            ('ttl_seconds', ttl_seconds), ('max_cursors', max_cursors)):
            if not isinstance(value, int) or value <= 0:
                raise ValueError('服务限制参数非法: %s' % name)
        if max_fragment_rows > max_buffered_rows:
            raise ValueError('片段行数上限不能超过暂存行数上限')
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.store = store
        self.secret = secret
        self.results = Results(store)
        self.page_rows = page_rows
        self.max_fragment_rows = max_fragment_rows
        self.max_buffered_rows = max_buffered_rows
        self.max_buffered_bytes = max_buffered_bytes
        self.ttl_seconds = ttl_seconds
        self.max_cursors = max_cursors

    # ---------- 状态文件与锁 ----------

    def _path(self, cursor_id):
        return self.root / ('%s.json' % cursor_id)

    @contextmanager
    def _registry_lock(self):
        """创建游标与清扫时使用的全服务锁。"""
        fd = os.open(self.root / '.registry.lock', os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    @contextmanager
    def _locked(self, cursor_id):
        """每游标锁；进程崩溃由操作系统释放，句柄与路径 inode 复核防误删竞争。"""
        path = self.root / ('.%s.lock' % cursor_id)
        fd = None
        while fd is None:
            candidate = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
            try:
                fcntl.flock(candidate, fcntl.LOCK_EX)
                if os.fstat(candidate).st_ino == os.stat(path).st_ino:
                    fd = candidate
                else:
                    fcntl.flock(candidate, fcntl.LOCK_UN)
                    os.close(candidate)
            except FileNotFoundError:
                # 锁文件在加锁期间被 sweep 删除，重新打开新文件。
                fcntl.flock(candidate, fcntl.LOCK_UN)
                os.close(candidate)
            except Exception:
                os.close(candidate)
                raise
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _read(self, cursor_id):
        path = self._path(cursor_id)
        try:
            state = json.loads(path.read_bytes())
        except FileNotFoundError:
            raise ValueError('游标不存在或已过期') from None
        if not isinstance(state, dict) or state.get('format') != 1:
            raise ValueError('游标状态格式不支持')
        return state

    def _save(self, state):
        fd, name = tempfile.mkstemp(dir=self.root, prefix='.state-')
        try:
            with os.fdopen(fd, 'wb') as stream:
                stream.write(encode(state))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, self._path(state['cursor_id']))
        finally:
            if os.path.exists(name):
                os.unlink(name)

    # ---------- 令牌 ----------

    def _token(self, state, position):
        return sign({'cursor': state['cursor_id'], 'query': state['identity'],
                     'position': position}, self.secret)

    def _payload(self, token):
        payload = verify(token, self.secret)
        if (not isinstance(payload, dict) or not isinstance(payload.get('cursor'), str)
                or not isinstance(payload.get('query'), str)
                or not isinstance(payload.get('position'), int)
                or isinstance(payload.get('position'), bool) or payload['position'] < 0):
            raise ValueError('令牌内容非法')
        return payload

    def _checked_state(self, payload, now):
        """载入状态并核对令牌身份与有效期；通过则滑动续期（调用方负责落盘）。"""
        state = self._read(payload['cursor'])
        if payload['query'] != state['identity']:
            raise ValueError('令牌与查询身份不符')
        if state.get('expired') or now > state['expires_at']:
            raise ValueError('游标不存在或已过期')
        state['expires_at'] = now + self.ttl_seconds
        return state

    # ---------- 生命周期 ----------

    def open(self, tenant, snapshot, parameters, order, *, now, cursor_id=None):
        """冻结查询身份并建立游标，返回位置 0 的续取令牌。

        指定 cursor_id 且已存在同身份游标时幂等返回当前位置的令牌。
        """
        if not isinstance(tenant, str) or not tenant:
            raise ValueError('租户身份非法')
        if not isinstance(snapshot, str) or not snapshot:
            raise ValueError('快照身份非法')
        if not isinstance(parameters, dict):
            raise ValueError('查询参数必须是对象')
        if not isinstance(order, list) or any(not isinstance(c, str) for c in order):
            raise ValueError('排序规则必须是列名列表')
        identity = fingerprint(tenant, snapshot, parameters, order)
        cursor_id = cursor_id or uuid.uuid4().hex
        with self._registry_lock():
            if self._path(cursor_id).exists():
                state = self._read(cursor_id)
                if state['identity'] != identity:
                    raise ValueError('游标身份冲突')
                if state.get('expired') or now > state['expires_at']:
                    raise ValueError('游标不存在或已过期')
                return self._token(state, state['confirmed'])
            if len(list(self.root.glob('*.json'))) >= self.max_cursors:
                raise ValueError('游标数量超限')
            state = {'format': 1, 'cursor_id': cursor_id, 'identity': identity,
                     'tenant': tenant, 'snapshot': snapshot, 'parameters': parameters,
                     'order': list(order), 'status': RUNNING, 'reason': None,
                     'confirmed': 0, 'base': 0, 'total_rows': 0, 'buffered_bytes': 0,
                     'fragments': [], 'landed': 0, 'last_key': None, 'pending': None,
                     'fragments_reclaimed': False, 'expired': False,
                     'created_at': now, 'expires_at': now + self.ttl_seconds}
            self._save(state)
        return self._token(state, 0)

    def describe(self, token, *, now):
        """游标当前状态快照（不含结果行）。"""
        payload = self._payload(token)
        with self._locked(payload['cursor']):
            state = self._checked_state(payload, now)
            self._save(state)
            return {'cursor': state['cursor_id'], 'identity': state['identity'],
                    'tenant': state['tenant'], 'snapshot': state['snapshot'],
                    'status': state['status'], 'reason': state['reason'],
                    'confirmed': state['confirmed'], 'total_rows': state['total_rows'],
                    'buffered_bytes': state['buffered_bytes'],
                    'pending_page': state['pending'],
                    'fragments_reclaimed': state['fragments_reclaimed'],
                    'created_at': state['created_at'], 'expires_at': state['expires_at']}

    def finish(self, token, *, now):
        """生产方声明结果已完整落地。"""
        return self._transition(token, now, RUNNING, COMPLETE, None)

    def fail(self, token, reason, *, now):
        """生产方声明执行失败；失败游标不再产出页，只报告 failed。"""
        if not isinstance(reason, str) or not reason:
            raise ValueError('失败原因非法')
        return self._transition(token, now, RUNNING, FAILED, reason)

    def cancel(self, token, *, now):
        """调用方取消查询；对象由 sweep 回收，墓碑保留到过期以报告 cancelled。"""
        payload = self._payload(token)
        with self._locked(payload['cursor']):
            state = self._read(payload['cursor'])
            if payload['query'] != state['identity']:
                raise ValueError('令牌与查询身份不符')
            if state.get('expired') or now > state['expires_at']:
                raise ValueError('游标不存在或已过期')
            if state['status'] != RUNNING:
                raise ValueError('查询已结束，不能取消')
            state['status'] = CANCELLED
            self._save(state)
            return {'status': state['status']}

    def _transition(self, token, now, source, target, reason):
        payload = self._payload(token)
        with self._locked(payload['cursor']):
            state = self._checked_state(payload, now)
            if state['status'] != source:
                raise ValueError('查询已结束，状态不能推进')
            state['status'] = target
            state['reason'] = reason
            self._save(state)
            return {'status': state['status'], 'reason': state['reason']}

    # ---------- 结果落地 ----------

    def land(self, token, fragment_id, rows, *, now):
        """把一段按物理全序排列的结果行落地为不可变片段。

        片段内及片段间的全序键必须严格递增（排序键并列由 occurrence_id 打破，
        因此重复的 occurrence_id 也会被拒绝），保证位置偏移永久稳定。
        """
        payload = self._payload(token)
        with self._locked(payload['cursor']):
            state = self._checked_state(payload, now)
            if state['status'] != RUNNING:
                raise ValueError('查询已结束，不能再写入片段')
            if not isinstance(rows, list) or not rows:
                raise ValueError('片段缺少行')
            if len(rows) > self.max_fragment_rows:
                raise ValueError('片段行数超限')
            size = len(encode(rows))
            if (state['total_rows'] + len(rows) > self.max_buffered_rows
                    or state['buffered_bytes'] + size > self.max_buffered_bytes):
                state['status'] = FAILED
                state['reason'] = '结果超出暂存限制'
                self._save(state)
                raise ValueError('结果超出暂存限制')
            keys = [row_key(state['order'], row) for row in rows]
            if any(keys[i] >= keys[i + 1] for i in range(len(keys) - 1)):
                raise ValueError('片段内排序键未严格递增')
            if state['last_key'] is not None:
                last = tuple(_comparable(value) for value in state['last_key'])
                if last >= keys[0]:
                    raise ValueError('片段排序键早于已落地结果')
            if fragment_id is None:
                fragment_id = 'frag-%08d' % state['landed']
            if any(fragment['fragment_id'] == fragment_id for fragment in state['fragments']):
                raise ValueError('片段身份重复')
            digest = self.results.put_fragment(state['identity'], fragment_id, rows,
                                               cursor=state['cursor_id'])
            state['fragments'].append({'fragment_id': fragment_id, 'digest': digest,
                                       'rows': len(rows), 'bytes': size})
            state['landed'] += 1
            state['total_rows'] += len(rows)
            state['buffered_bytes'] += size
            state['last_key'] = [rows[-1][column] for column in key_columns(state['order'])]
            self._save(state)
            return {'digest': digest, 'fragment_id': fragment_id,
                    'total_rows': state['total_rows']}

    def feed(self, token, rows, *, now):
        """流式落地：把可迭代行流按片段上限切块写入，内存只占用一个片段。"""
        batch = []
        count = 0
        for row in rows:
            batch.append(row)
            if len(batch) >= self.max_fragment_rows:
                self.land(token, None, batch, now=now)
                count += len(batch)
                batch = []
        if batch:
            self.land(token, None, batch, now=now)
            count += len(batch)
        return count

    def run_snapshot(self, token, snapshots, *, now):
        """对打开时冻结的快照执行查询：读出、按全序排序、流式落地并完结。

        排序在内存中进行，规模以快照为界；真正的执行引擎应把有序行流直接
        交给 feed。快照身份在 open 时冻结，之后新入仓的资料不会进入本游标。
        """
        payload = self._payload(token)
        with self._locked(payload['cursor']):
            state = self._checked_state(payload, now)
            snapshot = state['snapshot']
            order = list(state['order'])
        data = snapshots.read(snapshot)
        rows = []
        for row in data['rows']:
            row = dict(row)
            row.setdefault('occurrence_id', row.get('row_id'))
            rows.append(row)
        rows.sort(key=lambda row: row_key(order, row))
        self.feed(token, rows, now=now)
        self.finish(token, now=now)
        return {'rows': len(rows)}

    # ---------- 取页与确认 ----------

    def fetch(self, token, *, now):
        """按令牌位置取下一页。

        返回 status=page 时携带 rows 与下一位置的令牌；尚无下一页、已经结束、
        执行失败分别返回 pending / done / failed / cancelled。同一令牌重试
        返回同一页：未确认页在切页时已持久化。
        """
        payload = self._payload(token)
        with self._locked(payload['cursor']):
            state = self._checked_state(payload, now)
            position = payload['position']
            if state['status'] in (FAILED, CANCELLED):
                self._save(state)
                return self._empty(state)
            confirmed = state['confirmed']
            if position < confirmed:
                raise ValueError('令牌位置已确认')
            if position > confirmed:
                raise ValueError('令牌位置越过未确认页')
            pending = state['pending']
            if pending is None:
                available = state['total_rows'] - confirmed
                if available <= 0:
                    self._save(state)
                    return self._empty(state)
                pending = {'position': confirmed,
                           'length': min(self.page_rows, available)}
                state['pending'] = pending
            self._save(state)
            rows = self._read_rows(state, pending['position'], pending['length'])
            return {'status': PAGE, 'position': pending['position'], 'rows': rows,
                    'next': self._token(state, pending['position'] + pending['length']),
                    'total_rows': state['total_rows'], 'expires_at': state['expires_at']}

    def ack(self, token, *, now):
        """确认推进：令牌位置必须与未决页的终点一致；重复确认幂等。

        确认之后，完全落在确认位置之前的片段立即回收，暂存空间只保留
        未确认的尾部。
        """
        payload = self._payload(token)
        with self._locked(payload['cursor']):
            state = self._checked_state(payload, now)
            position = payload['position']
            pending = state['pending']
            if position == state['confirmed'] and pending is None:
                self._save(state)
                return {'confirmed': state['confirmed']}
            if pending is None or position != pending['position'] + pending['length']:
                raise ValueError('确认位置与未决页不符')
            state['confirmed'] = position
            state['pending'] = None
            released = self._pop_confirmed(state)
            self._save(state)
            for digest in released:
                self.store.delete(digest)
            return {'confirmed': position}

    def _pop_confirmed(self, state):
        """从状态里摘除已确认位置之前的片段，返回待删对象摘要。"""
        released = []
        offset = state['base']
        kept = list(state['fragments'])
        while kept and offset + kept[0]['rows'] <= state['confirmed']:
            fragment = kept.pop(0)
            offset += fragment['rows']
            released.append(fragment['digest'])
            state['buffered_bytes'] -= fragment['bytes']
        state['fragments'] = kept
        state['base'] = offset
        return released

    def _read_rows(self, state, start, length):
        rows = []
        offset = state['base']
        for fragment in state['fragments']:
            end = offset + fragment['rows']
            if end <= start:
                offset = end
                continue
            if offset >= start + length:
                break
            data = self.results.read_fragment(fragment['digest'], state['identity'],
                                              cursor=state['cursor_id'])
            rows.extend(data[max(0, start - offset):min(fragment['rows'],
                                                       start + length - offset)])
            offset = end
        if len(rows) != length:
            raise ValueError('结果片段缺失或长度不符')
        return rows

    def _empty(self, state):
        status = {RUNNING: PENDING, COMPLETE: DONE,
                  FAILED: FAILED, CANCELLED: CANCELLED}[state['status']]
        result = {'status': status, 'position': state['confirmed'],
                  'total_rows': state['total_rows']}
        if state['reason']:
            result['reason'] = state['reason']
        return result

    # ---------- 过期清理与回收 ----------

    def sweep(self, *, now):
        """回收过期游标与失败、取消、已取完游标的暂存对象。

        顺序为崩溃安全：先落盘标记，再删对象，最后删状态文件。持有游标锁的
        合法读取与 sweep 互斥，不会读到半新半旧的页。
        """
        reclaimed = {'cursors': [], 'objects': 0, 'errors': []}
        with self._registry_lock():
            for path in sorted(self.root.glob('*.json')):
                cursor_id = path.stem
                try:
                    with self._locked(cursor_id):
                        state = self._read(cursor_id)
                        expired = state.get('expired') or now > state['expires_at']
                        drained = (state['status'] in (FAILED, CANCELLED)
                                   or (state['status'] == COMPLETE
                                       and state['confirmed'] == state['total_rows']))
                        if (expired or drained) and not state['fragments_reclaimed']:
                            state['fragments_reclaimed'] = True
                            if expired:
                                state['expired'] = True
                            state['buffered_bytes'] = 0
                            self._save(state)
                            for fragment in state['fragments']:
                                if self.store.delete(fragment['digest']):
                                    reclaimed['objects'] += 1
                        if expired:
                            path.unlink(missing_ok=True)
                            (self.root / ('.%s.lock' % cursor_id)).unlink(missing_ok=True)
                            reclaimed['cursors'].append(cursor_id)
                except ValueError:
                    reclaimed['errors'].append(cursor_id)
        return reclaimed


def demo(data, store, root):
    """在合成资料上演示续取游标：冻结快照、增量落地、断点重取与终态区分。"""
    from .snapshot import Snapshots

    query = data['queries'][0]
    service = CursorService(root / 'cursors', store, b'demo-secret',
                            page_rows=2, ttl_seconds=600)
    rows = [dict(row, row_id=row['occurrence_id'])
            for fragment in data['fragments'] for row in fragment['rows']]
    snapshots = Snapshots(store)
    snapshot = snapshots.create(rows, {'time': 'int', 'value': 'int'})
    token = service.open(query['tenant'], snapshot, query['parameters'],
                         query['order'], now=1000)
    # 游标冻结后新入仓的资料进入更新的快照，不影响本游标。
    snapshots.create(rows + [{'row_id': 'z', 'occurrence_id': 'z', 'time': 5, 'value': 9}],
                     {'time': 'int', 'value': 'int'})
    ordered = sorted(rows, key=lambda row: row_key(query['order'], row))
    service.feed(token, ordered[:2], now=1001)
    first = service.fetch(token, now=1002)
    retry = service.fetch(token, now=1003)
    service.ack(first['next'], now=1004)
    pending = service.fetch(first['next'], now=1005)
    service.feed(token, ordered[2:], now=1006)
    service.finish(token, now=1007)
    second = service.fetch(first['next'], now=1008)
    service.ack(second['next'], now=1009)
    done = service.fetch(second['next'], now=1010)
    pages = [first['rows'], second['rows']]
    return {'query_id': service.describe(token, now=1011)['identity'],
            'statuses': [first['status'], pending['status'],
                         second['status'], done['status']],
            'retry_same_page': retry['rows'] == first['rows'],
            'page_rows': [len(page) for page in pages],
            'rows': [row['occurrence_id'] for page in pages for row in page]}
