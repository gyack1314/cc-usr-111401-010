# 长查询结果资料工具

Python 3.11 项目，用于读取、保存及核对协作资料仓的本地资料。不可变内容通过 SHA256 寻址，目录引用单独保存。现有领域接口位于 warehouse/cursor.py，资料契约见 docs/contracts.md，合成输入位于 data/source.json。warehouse/objects.py 负责内容校验，catalog.py 负责本地引用更新，snapshot.py 提供完整行快照基础读取。

warehouse/paging.py 提供可持久续取的结果游标服务 CursorService：打开游标时冻结快照、参数与排序规则（指纹即查询身份），结果按物理全序追加式落地为不可变片段；页令牌经 HMAC 签名并绑定游标身份与已确认位置，取页与确认分离，客户端断线后可用同一令牌重取同一页，跨进程重建服务即可继续。取页状态区分 page / pending / done / failed / cancelled；确认推进即回收已确认片段，过期或取消的游标由 sweep 崩溃安全地回收对象。页行数、片段行数、暂存行数与字节、游标数量均有硬上限。

已有读取流程使用临时目录，不改变源资料。当前只提供源码中已实现的基础操作，输出相应对象身份、行或引用摘要。领域接口的测试说明其现有边界，不承诺额外的管理或查询功能。

运行与开发检查：

```sh
python -m unittest discover -s tests -v
python -m compileall -q warehouse tests
python -m warehouse
python -m warehouse --cursor-demo   # 演示续取游标：冻结快照、增量落地、断点重取
```
