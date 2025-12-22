# CosyVoice 多进程自动部署服务

基于 CosyVoice2-0.5B 的分布式语音合成服务，提供完善的多进程管理、自动故障恢复和 RESTful API 接口。

如下内容基于 Ai 生成。

## 系统架构

本系统采用分布式多进程架构，通过 Redis 消息队列实现任务分发和状态管理，主要包含三个核心组件：

### 1. API 主服务层
- 基于 FastAPI 的异步 HTTP 服务
- 负责请求验证、任务分发和结果聚合
- 进程生命周期管理和健康监控
- GPU 资源分配和部署计划计算

### 2. 文本分割进程（Splitter）
- 单进程运行，全局唯一
- 长文本归一化和智能分句
- 子任务生成和分发
- 轻量级前端处理器，显存占用低

### 3. TTS 合成进程（Worker）
- 多进程并行运行，绑定到不同 GPU
- 加载 CosyVoice2 模型执行推理
- 预加载提示音，支持零样本克隆
- 自动重试机制和死信队列管理

### 数据流转

```
客户端请求
    |
    v
[API 服务]
    | - 验证 API Key
    | - 创建任务 ID
    v
[Redis: SPLITTER_TASK_QUEUE]
    |
    v
[Splitter 进程]
    | - 文本分割
    | - 生成子任务
    v
[Redis: WORKER_TASK_QUEUE]
    |
    v
[Worker 进程池] (GPU-0, GPU-1, ...)
    | - 并行合成
    | - 保存结果
    v
[Redis: 完成通知]
    |
    v
[API 服务]
    | - 音频合并
    | - 返回下载 URL
    v
客户端接收结果
```

## 核心特性

### 高可用性
- 进程异常自动监测和重启
- 处理中任务自动恢复到队列
- 失败重试机制（可配置次数和延迟）
- 死信队列记录永久失败任务

### GPU 智能调度
- 显存感知部署：根据 GPU 可用显存动态分配进程
- 均匀分配模式：多进程平均分布到各 GPU
- 单进程绑定单 GPU，避免资源竞争

### 分布式任务队列
- 基于 Redis 的可靠队列机制
- BRPOPLPUSH 原子操作保证任务不丢失
- Pub/Sub 实现进程间通信
- 结果自动过期清理（TTL）

### 优雅关闭
- 广播停止信号到所有子进程
- 等待处理中任务完成
- 强制终止未响应进程
- 确保资源正确释放

## 环境要求

### 硬件要求
- NVIDIA GPU（支持 CUDA）
- 单模型建议显存：5GB+
- 多进程部署建议：16GB+ 显存

### 软件依赖
- Python 3.8+
- CUDA 11.0+
- Redis 5.0+

### Python 依赖库
```
fastapi
uvicorn[standard]
pydantic
pydantic-settings
redis
torch
torchaudio
librosa
numpy
hyperpyyaml
pynvml
```

## 安装部署

### 1. 克隆项目
```bash
git clone <repository_url>
cd reCosyVoiceService
```

### 2. 安装依赖
```bash
pip install -r requirements.txt
```

### 3. 下载模型文件
将 CosyVoice2-0.5B 预训练模型放置到指定目录：
```
pretrained_models/CosyVoice2-0.5B/
```

### 4. 准备提示音文件
根据配置文件准备音色提示音：
```
prompts/
├── default_female.wav
├── announcer_male.wav
└── ...
```

### 5. 配置 Redis
确保 Redis 服务正常运行：
```bash
redis-server
```

### 6. 配置环境变量
复制配置模板并编辑：
```bash
cp env.example .env
```

编辑 `.env` 文件（参考配置说明章节）。

### 7. 启动服务
```bash
python main.py
```

服务默认运行在 `http://0.0.0.0:8000`。

## 配置说明

### 模型与资源配置
```bash
# 模型路径
MODEL_PATH="pretrained_models/CosyVoice2-0.5B"

# GPU 显存管理
# 正数：每个模型需要的显存（MB），用于显存感知部署
# -1：均匀分配模式
GPU_MEMORY_PER_MODEL_MB=5000

# 工作进程总数
NUM_MODELS=8
```

### 提示音配置
```bash
PROMPTS_CONFIG='[
    {
        "name": "announcer_female",
        "path": "prompts/default_female.wav",
        "text": "这是提示音对应的原始文本内容"
    },
    {
        "name": "announcer_male",
        "path": "prompts/announcer_male.wav",
        "text": "提示音文本内容"
    }
]'
```

### Redis 配置
```bash
REDIS_HOST="localhost"
REDIS_PORT=6379
REDIS_DB=0
REDIS_PREFIX="cosyvoice:"
RESULT_EXPIRY_SECONDS=3600
```

### 工作进程配置
```bash
# 失败重试次数
WORKER_RETRY_ATTEMPTS=3

# 重试延迟（秒）
WORKER_RETRY_DELAY_SECONDS=2
```

### API 安全配置
```bash
# API 密钥白名单（JSON 数组）
VALID_API_KEYS='["admin", "key1", "key2"]'
```

### 目录配置
```bash
STATIC_DIR="static/audio"
TEMP_AUDIO_DIR="static/audio/temp_chunks"
TEMP_VOICE_DIR="static/audio/temp_voices"
```

## API 使用说明

### 端点列表

| 方法 | 路由 | 功能 |
|------|------|------|
| GET | `/` | 服务状态检查 |
| GET | `/voices` | 获取可用音色列表 |
| POST | `/synthesize` | 合成音频 |
| GET | `/audio/{filename}` | 下载合成音频 |

### 1. 检查服务状态
```bash
curl http://localhost:8000/
```

响应示例：
```json
{
  "status": "就绪",
  "available_voices": ["announcer_female", "announcer_male"]
}
```

### 2. 获取音色列表
```bash
curl http://localhost:8000/voices
```

响应示例：
```json
["announcer_female", "announcer_male", "meta1"]
```

### 3. 合成音频（使用预设音色）
```bash
curl -X POST http://localhost:8000/synthesize \
  -H "Content-Type: application/json" \
  -d '{
    "apikey": "admin",
    "target": "这是需要合成的文本内容",
    "voice": "announcer_female"
  }'
```

响应示例：
```json
{
  "download_url": "http://localhost:8000/audio/550e8400-e29b-41d4-a716-446655440000.wav",
  "processing_time_seconds": 5.42,
  "num_chunks": 3
}
```

### 4. 合成音频（使用即时克隆）
```bash
curl -X POST http://localhost:8000/synthesize \
  -H "Content-Type: application/json" \
  -d '{
    "apikey": "admin",
    "target": "这是需要合成的文本内容",
    "voice_url": "https://example.com/sample_audio.wav",
    "voice_prompt_text": "这是示例音频的原始文本内容"
  }'
```

### 请求参数说明

| 参数 | 类型 | 必需 | 说明 |
|------|------|------|------|
| `apikey` | string | 是 | API 密钥 |
| `target` | string | 是 | 待合成的目标文本 |
| `voice` | string | 条件必需 | 预设音色名称（与 voice_url 二选一） |
| `voice_url` | string | 条件必需 | 外部音频 URL（与 voice 二选一） |
| `voice_prompt_text` | string | 条件必需 | voice_url 对应的文本（使用 voice_url 时必需） |

### 错误响应

| 状态码 | 说明 |
|--------|------|
| 400 | 参数验证失败或音色不存在 |
| 401 | API 密钥无效 |
| 500 | 服务内部错误 |
| 503 | 服务未就绪 |
| 504 | 任务处理超时（600秒） |

## 运维监控

### 日志查看
服务日志输出格式：
```
[时间戳] [日志级别] [模块名] [进程ID] 日志内容
```

关键日志：
- 进程启动：`Starting Splitter...` / `Starting Worker-GPU{id}-{idx}...`
- 进程重启：`Restarting Worker-GPU{id}-{idx} after unexpected exit`
- 任务恢复：`Recovered N pending tasks from processing queue`
- 任务失败：`Task failed after X attempts, moving to dead letter queue`

### Redis 队列监控

#### 查看队列长度
```bash
# 待分割任务
redis-cli LLEN cosyvoice:splitter_tasks

# 待合成任务
redis-cli LLEN cosyvoice:worker_tasks

# 死信队列
redis-cli LLEN cosyvoice:dead_letter_queue
```

#### 查看处理中任务
```bash
# 列出所有处理队列
redis-cli KEYS "cosyvoice:processing:*"

# 查看特定处理队列长度
redis-cli LLEN cosyvoice:processing:worker-0
```

#### 查看死信任务
```bash
# 获取最新失败任务
redis-cli LINDEX cosyvoice:dead_letter_queue 0

# 查看所有失败任务
redis-cli LRANGE cosyvoice:dead_letter_queue 0 -1
```

### GPU 监控
```bash
# 实时监控 GPU 使用情况
nvidia-smi -l 1

# 查看显存占用
nvidia-smi --query-gpu=index,name,memory.used,memory.total --format=csv
```

### 性能调优建议

#### GPU 部署策略
- 多卡均衡：设置 `GPU_MEMORY_PER_MODEL_MB=-1`
- 显存优化：设置 `GPU_MEMORY_PER_MODEL_MB` 为实际模型显存需求
- 进程数量：`NUM_MODELS` 设置为 GPU 总显存/单模型显存

#### 并发配置
- 增加 `NUM_MODELS` 提高并发能力
- 调整 `WORKER_RETRY_ATTEMPTS` 平衡容错性和响应速度
- 增加 `RESULT_EXPIRY_SECONDS` 避免频繁清理缓存

#### 任务超时
API 请求默认超时 600 秒，适用于长文本合成。如需调整，修改 `api/main.py` 中 `synthesize_audio` 函数的超时参数。

### 故障排查

#### 服务启动失败
检查项：
1. Redis 连接是否正常
2. 模型文件路径是否正确
3. 提示音文件是否存在
4. GPU 是否可用（`nvidia-smi`）
5. CUDA 环境变量是否正确

#### 合成失败
检查项：
1. 查看死信队列：`redis-cli LRANGE cosyvoice:dead_letter_queue 0 -1`
2. 检查 Worker 进程日志
3. 验证提示音文件格式和内容
4. 确认 GPU 显存充足

#### 进程频繁重启
可能原因：
1. GPU 显存不足，OOM
2. 模型加载失败
3. Redis 连接不稳定
4. 系统资源耗尽

## 技术栈

### Web 框架
- FastAPI：高性能异步 Web 框架
- Uvicorn：ASGI 服务器
- Pydantic：数据验证和配置管理

### 消息队列
- Redis：任务队列和状态存储
- Pub/Sub：进程间通信

### 深度学习
- PyTorch：神经网络推理引擎
- CosyVoice2-0.5B：语音合成模型
- torchaudio：音频张量处理

### 音频处理
- librosa：音频分析和处理
- 采样率：22050 Hz
- 格式：WAV（PCM 16-bit）

### 进程管理
- multiprocessing：Python 多进程
- pynvml：NVIDIA GPU 监控

### 其他
- hyperpyyaml：YAML 配置解析
- numpy：数值计算

## 架构优势

1. **高吞吐量**：多进程并行合成，充分利用多卡 GPU
2. **容错性强**：进程自动重启、任务自动恢复、失败重试机制
3. **可扩展性**：支持动态调整进程数量，水平扩展能力强
4. **低延迟**：异步处理、提示音预加载、结果缓存
5. **运维友好**：详细日志、Redis 监控、优雅关闭

## License

请遵循 CosyVoice 官方项目的开源协议。

## 联系方式

如有问题或建议，请提交 Issue 或 Pull Request。
