**为了消除歧义，原先被称为API的模块现在一律改称为facade。不明白facade是什么意思的可以去看shared resources 里面的facade.pdf**

###文件组织（重要）

所有数据库相关存放于./schema目录下

./schema/metadata.pickle是数据库的metadata

所有表存放在./schema/tables目录的**子目录**下，每个表和相关索引放在同一个目录下，表的文件为XXX.table，索引文件为XXX.index

所有主键索引命名为PRIMARY.index

如spam表和spammer索引，存放为：

./schema/tables/spam/spam.table

./schema/tables/spam/spammer.index

./schema/tables/spam/PRIMARY.index

###资源
**在shared resources目录下是共享的开发资源**

5/31 新增facade.pdf，是Head First Design Patterns里面摘出的一部分，API模块将实现其中提到的功能

里面有一本python cookbook 3e

由于这个项目需要处理大量二进制文件，难度比较大，大家都去看一下其中的5.9, 5.10, 6.11, 6.12这几章

-------
    6月24日更新：
    
    - 验收的时候用scripts文件夹中的**test_all.txt**，该文件中包含了所有需要测试的指令，只是插入的数据量比较少。

    - scripts文件夹中的**test1.txt**尝试插入大量数据，但是在插入超过59条数据时B+树结点的分裂中抛出了不知道什么意思的异常。暂时还不知道是为什么。