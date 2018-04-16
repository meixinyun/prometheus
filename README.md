为了方便利用 prometheus 的查询语法 ，灵活的告警规则函数， 尝试对prometheus的源码做一些改造，以便支持如下功能

1.  支持falcon 的 agent 上报数据，将 falcon 的数据模型映射为 prometheus的数据模型

　   tags   对应 prometheus 的 label 
    
2.  提供 falcon 类型的 http  Post 数据接口