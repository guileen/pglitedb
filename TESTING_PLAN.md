# 兼容性测试方案

## 1. 测试目标

确保将内部实现移到 `internal/` 目录后，外部客户端仍然可以正常使用所有公开API，不会破坏现有功能。

## 2. 测试范围

### 2.1 客户端接口测试
- 验证 `client.Client` 结构体的所有公共方法正常工作
- 验证 `NewClient` 和 `NewClientWithExecutor` 工厂函数正常工作
- 验证所有 CRUD 操作：Insert, Select, Update, Delete

### 2.2 包导入测试
- 验证主模块的 `go.mod` 文件正确引用了内部包
- 验证外部项目可以通过 `github.com/guileen/pglitedb/client` 正常导入和使用

### 2.3 功能完整性测试
- 验证所有数据类型支持（字符串、数字、布尔值等）
- 验证查询选项（过滤、排序、限制等）正常工作
- 验证多租户支持正常工作

## 3. 测试环境

### 3.1 主项目测试
```
go test ./...
```

### 3.2 Example项目测试
```bash
cd examples/compatibility_test
go mod tidy
go run main.go
go test
```

## 4. 测试用例

### 4.1 基本功能测试
1. 创建客户端实例
2. 插入数据记录
3. 查询数据记录
4. 更新数据记录
5. 删除数据记录

### 4.2 边界条件测试
1. 空数据插入
2. 大数据量查询
3. 并发访问测试
4. 错误处理验证

### 4.3 兼容性测试
1. 不同Go版本兼容性
2. 不同操作系统兼容性
3. 向后兼容性验证

## 5. 自动化测试

### 5.1 CI/CD集成
- 在GitHub Actions中添加兼容性测试工作流
- 测试不同Go版本（1.23, 1.24, 1.25）
- 测试不同平台（Linux, macOS, Windows）

### 5.2 测试脚本
创建自动化测试脚本：
```bash
#!/bin/bash
# test_compatibility.sh

echo "Running main project tests..."
go test ./... -v

echo "Running compatibility example..."
cd examples/compatibility_test
go mod tidy
go run main.go
go test -v

echo "Compatibility tests completed!"
```

## 6. 验收标准

- 所有现有测试通过
- Example项目能够正常编译和运行
- 客户端API没有破坏性变更
- 内部实现成功隐藏在 `internal/` 目录中