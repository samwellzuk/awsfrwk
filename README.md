# awsfrwk

以下函数通过装饰器封装对被装饰函数的并发调用。
通过函数的taskid/funcid等信息可以达到对每个函数并发控制的目的。即：相同 taskid-funcid-awsfunc 作为一个并发调用的单元。
内部会根据这个来自动建立对应的aws lambda函数，aws sqs队列
