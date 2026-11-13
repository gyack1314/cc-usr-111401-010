# 资料与既有接口约定

结果每行带occurrence_id，用于区分值完全相同的结果实例；sort_key允许相同，物理全序再按occurrence_id打破并列。结果片段有query_id、fragment_id、digest、rows，终态独立为running、complete、failed或cancelled。逻辑时钟显式传入，不使用sleep测试过期。cursor.py已有查询指纹和HMAC令牌签验，results.py仅支持片段写入和校验读取，均未实现页、确认或回收；服务secret由调用者运行时给入，不落公开数据。

所有资料均为合成，字段、身份及状态按本文解释。源码中的 inspect 入口只读取并核对现有基础接口；不会建立高级查询或仓库管理流程。

# 可续取结果游标约定

paging.py 的 CursorService 在既有接口上实现分页协议：

- 打开游标即冻结 tenant、snapshot、parameters、order，四者指纹为查询身份；期间新入仓的资料只进入更新的快照，不会插入既有游标的结果。
- 结果按物理全序（order 列加 occurrence_id 打破并列）追加式落地；片段内与片段间的全序键必须严格递增，落地时校验，违背即拒绝。排序键并列与值重复的行按 occurrence_id 区分并完整保留。
- 页令牌为 HMAC 签名的 {cursor, query, position}，绑定游标身份与已确认位置；其他租户或参数组的指纹不同，无法续取。secret 由调用者运行时给入。
- 协议为 fetch → ack → fetch：fetch 只在已确认位置切页并持久化未决页，同一令牌重试返回同一页；ack 携带上一页返回的下一位置令牌，位置须与未决页终点一致，重复确认幂等。
- fetch 返回 page（含 rows 与下一令牌）或 pending（运行中暂无下一页）/ done（已完结且取尽）/ failed / cancelled；暂时没有片段不是终点。
- 确认推进即回收前沿：完全落在确认位置之前的片段立即删除，暂存只保留未确认尾部。取消或过期（滑动 ttl，逻辑时钟 now 显式传入）的游标由 sweep 按"先落盘标记、再删对象、后删状态"的顺序回收；failed / cancelled 游标的对象同样可回收，状态墓碑保留到过期以报告终态。
- 每游标 flock 串行化 fetch / ack / land / sweep，进程崩溃由操作系统释放锁；片段不可变且按 cursor_id 归属隔离，合法读取不会拿到半新半旧的页。
- 资源硬上限：page_rows（页行数）、max_fragment_rows（片段行数）、max_buffered_rows / max_buffered_bytes（暂存行数与字节，超限则游标转为 failed）、max_cursors（游标数量）。feed 流式落地只占一个片段的内存。
