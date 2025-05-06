# 简单的测试
curl -X POST "http://localhost:8000/v1/cache/string/test_key" -H "Content-Type: application/json" -d '{"value":"hello","ttl_seconds":60}'

curl http://localhost:8000/v1/cache/string/test_key

curl -X DELETE "http://localhost:8000/v1/cache/string/test_key" 

