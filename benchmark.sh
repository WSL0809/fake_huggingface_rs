#!/bin/bash

# 性能测试脚本 - 针对fake_huggingface_rs
# 测试下载性能、并发性能、Range请求性能

set -e

# 配置
SERVER_URL="http://localhost:8000"
TEST_REPO="tencent/HunyuanImage-2.1"
TEST_FILE="dit/hunyuanimage2.1.safetensors"  # 大文件
SMALL_FILE="README.md"  # 小文件
CONCURRENT_REQUESTS=10
TEST_DURATION=30

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Fake HuggingFace RS 性能测试 ===${NC}"
echo "服务器: $SERVER_URL"
echo "测试仓库: $TEST_REPO"
echo ""

# 检查服务器是否运行
check_server() {
    echo -e "${YELLOW}检查服务器状态...${NC}"
    if ! curl -s "$SERVER_URL/api/models/$TEST_REPO" > /dev/null; then
        echo -e "${RED}错误: 服务器未运行或无法访问${NC}"
        echo "请先启动服务器: RUST_LOG=info FAKE_HUB_ROOT=fake_hub cargo run"
        exit 1
    fi
    echo -e "${GREEN}✓ 服务器运行正常${NC}"
}

# 安装测试工具
install_tools() {
    echo -e "${YELLOW}检查测试工具...${NC}"
    
    # 检查curl
    if ! command -v curl &> /dev/null; then
        echo -e "${RED}错误: 需要安装curl${NC}"
        exit 1
    fi
    
    # 检查wrk (如果可用)
    if ! command -v wrk &> /dev/null; then
        echo -e "${YELLOW}警告: 未安装wrk，将使用curl进行基础测试${NC}"
        echo "安装wrk: brew install wrk (macOS) 或 apt-get install wrk (Ubuntu)"
    fi
    
    # 检查hey (如果可用)
    if ! command -v hey &> /dev/null; then
        echo -e "${YELLOW}提示: 安装hey可获得更好的负载测试: go install github.com/rakyll/hey@latest${NC}"
    fi
    
    echo -e "${GREEN}✓ 工具检查完成${NC}"
}

# 基础功能测试
test_basic_functionality() {
    echo -e "${YELLOW}=== 基础功能测试 ===${NC}"
    
    # 测试模型信息获取
    echo "测试模型信息获取..."
    start_time=$(date +%s%N)
    response=$(curl -s -w "%{http_code}" "$SERVER_URL/api/models/$TEST_REPO")
    end_time=$(date +%s%N)
    duration=$(( (end_time - start_time) / 1000000 ))
    
    if [[ "$response" == *"200" ]]; then
        echo -e "${GREEN}✓ 模型信息获取成功 (${duration}ms)${NC}"
    else
        echo -e "${RED}✗ 模型信息获取失败${NC}"
    fi
    
    # 测试文件HEAD请求
    echo "测试文件HEAD请求..."
    start_time=$(date +%s%N)
    response=$(curl -s -I -w "%{http_code}" "$SERVER_URL/$TEST_REPO/resolve/main/$TEST_FILE")
    end_time=$(date +%s%N)
    duration=$(( (end_time - start_time) / 1000000 ))
    
    if [[ "$response" == *"200" ]]; then
        echo -e "${GREEN}✓ HEAD请求成功 (${duration}ms)${NC}"
        # 提取文件大小
        file_size=$(echo "$response" | grep -i "content-length" | cut -d' ' -f2 | tr -d '\r')
        echo "  文件大小: $file_size bytes"
    else
        echo -e "${RED}✗ HEAD请求失败${NC}"
    fi
}

# 下载性能测试
test_download_performance() {
    echo -e "${YELLOW}=== 下载性能测试 ===${NC}"
    
    # 小文件下载测试
    echo "测试小文件下载..."
    start_time=$(date +%s%N)
    curl -s -o /dev/null "$SERVER_URL/$TEST_REPO/resolve/main/$SMALL_FILE"
    end_time=$(date +%s%N)
    duration=$(( (end_time - start_time) / 1000000 ))
    echo -e "${GREEN}✓ 小文件下载完成 (${duration}ms)${NC}"
    
    # 大文件下载测试（前1MB）
    echo "测试大文件Range下载 (前1MB)..."
    start_time=$(date +%s%N)
    curl -s -H "Range: bytes=0-1048575" -o /dev/null "$SERVER_URL/$TEST_REPO/resolve/main/$TEST_FILE"
    end_time=$(date +%s%N)
    duration=$(( (end_time - start_time) / 1000000 ))
    echo -e "${GREEN}✓ Range下载完成 (${duration}ms)${NC}"
    
    # 计算下载速度
    if [ $duration -gt 0 ]; then
        speed=$(( 1048576 * 1000 / duration / 1024 ))  # KB/s
        echo "  下载速度: ${speed} KB/s"
    fi
}

# Range请求测试
test_range_requests() {
    echo -e "${YELLOW}=== Range请求测试 ===${NC}"
    
    # 测试不同的Range请求
    ranges=("0-1023" "1024-2047" "0-" "-1024")
    
    for range in "${ranges[@]}"; do
        echo "测试Range: bytes=$range"
        start_time=$(date +%s%N)
        response=$(curl -s -H "Range: bytes=$range" -w "%{http_code}" "$SERVER_URL/$TEST_REPO/resolve/main/$TEST_FILE")
        end_time=$(date +%s%N)
        duration=$(( (end_time - start_time) / 1000000 ))
        
        if [[ "$response" == *"206" ]]; then
            echo -e "${GREEN}✓ Range请求成功 (${duration}ms)${NC}"
        else
            echo -e "${RED}✗ Range请求失败${NC}"
        fi
    done
}

# 并发测试
test_concurrency() {
    echo -e "${YELLOW}=== 并发测试 ===${NC}"
    
    if command -v wrk &> /dev/null; then
        echo "使用wrk进行并发测试..."
        wrk -t4 -c$CONCURRENT_REQUESTS -d${TEST_DURATION}s --timeout 10s \
            "$SERVER_URL/$TEST_REPO/resolve/main/$SMALL_FILE" \
            | grep -E "(Requests/sec|Transfer/sec|Latency)"
    elif command -v hey &> /dev/null; then
        echo "使用hey进行并发测试..."
        hey -n 1000 -c $CONCURRENT_REQUESTS "$SERVER_URL/$TEST_REPO/resolve/main/$SMALL_FILE"
    else
        echo "使用curl进行简单并发测试..."
        echo "启动 $CONCURRENT_REQUESTS 个并发请求..."
        
        start_time=$(date +%s%N)
        for i in $(seq 1 $CONCURRENT_REQUESTS); do
            curl -s -o /dev/null "$SERVER_URL/$TEST_REPO/resolve/main/$SMALL_FILE" &
        done
        wait
        end_time=$(date +%s%N)
        duration=$(( (end_time - start_time) / 1000000 ))
        
        echo -e "${GREEN}✓ 并发测试完成 (${duration}ms)${NC}"
        echo "  平均响应时间: $(( duration / CONCURRENT_REQUESTS ))ms"
    fi
}

# 内存和CPU监控
monitor_resources() {
    echo -e "${YELLOW}=== 资源监控 ===${NC}"
    
    if command -v top &> /dev/null; then
        echo "当前系统资源使用情况:"
        top -l 1 | grep -E "(CPU usage|PhysMem)"
    fi
    
    if command -v ps &> /dev/null; then
        echo "服务器进程信息:"
        ps aux | grep fake_huggingface_rs | grep -v grep || echo "未找到服务器进程"
    fi
}

# 缓存性能测试
test_cache_performance() {
    echo -e "${YELLOW}=== 缓存性能测试 ===${NC}"
    
    # 第一次请求（冷缓存）
    echo "第一次请求（冷缓存）..."
    start_time=$(date +%s%N)
    curl -s -o /dev/null "$SERVER_URL/api/models/$TEST_REPO"
    end_time=$(date +%s%N)
    cold_duration=$(( (end_time - start_time) / 1000000 ))
    
    # 第二次请求（热缓存）
    echo "第二次请求（热缓存）..."
    start_time=$(date +%s%N)
    curl -s -o /dev/null "$SERVER_URL/api/models/$TEST_REPO"
    end_time=$(date +%s%N)
    hot_duration=$(( (end_time - start_time) / 1000000 ))
    
    echo -e "${GREEN}✓ 缓存测试完成${NC}"
    echo "  冷缓存: ${cold_duration}ms"
    echo "  热缓存: ${hot_duration}ms"
    
    if [ $cold_duration -gt 0 ] && [ $hot_duration -gt 0 ]; then
        speedup=$(( cold_duration / hot_duration ))
        echo "  缓存加速比: ${speedup}x"
    fi
}

# 主函数
main() {
    check_server
    install_tools
    echo ""
    
    test_basic_functionality
    echo ""
    
    test_download_performance
    echo ""
    
    test_range_requests
    echo ""
    
    test_concurrency
    echo ""
    
    test_cache_performance
    echo ""
    
    monitor_resources
    echo ""
    
    echo -e "${GREEN}=== 性能测试完成 ===${NC}"
    echo ""
    echo -e "${YELLOW}性能优化建议:${NC}"
    echo "1. 监控内存使用，避免大文件缓存导致OOM"
    echo "2. 调整CHUNK_SIZE (当前256KB) 以平衡内存和网络效率"
    echo "3. 考虑启用HTTP/2支持以提高并发性能"
    echo "4. 监控缓存命中率，调整缓存TTL和容量"
    echo "5. 使用连接池优化数据库/文件系统访问"
}

# 运行主函数
main "$@"
