# 资料与既有接口约定

结果每行带occurrence_id，用于区分值完全相同的结果实例；sort_key允许相同，物理全序再按occurrence_id打破并列。结果片段有query_id、fragment_id、digest、rows，终态独立为running、complete、failed或cancelled。逻辑时钟显式传入，不使用sleep测试过期。cursor.py已有查询指纹和HMAC令牌签验，results.py仅支持片段写入和校验读取，均未实现页、确认或回收；服务secret由调用者运行时给入，不落公开数据。

所有资料均为合成，字段、身份及状态按本文解释。源码中的 inspect 入口只读取并核对现有基础接口；不会建立高级查询或仓库管理流程。
