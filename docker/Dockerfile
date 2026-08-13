# ============================================================
# Dockerfile（放在專案根目錄底下的 docker/ 資料夾）
# ============================================================
#
# Build 指令（在專案根目錄執行）：
#   docker build -f docker/Dockerfile -t prefect-demo:latest .
#
# 為什麼 build context 要在專案根目錄（.）？
#   因為要 COPY 整個專案的程式碼進 image
# ============================================================

# ── Base Image ──────────────────────────────────────────────
FROM python:3.12-slim

WORKDIR /app

# ── 安裝 uv（與本機工具鏈一致）────────────────────────────
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# ── 複製依賴設定（先複製，利用 Docker layer cache）─────────
# 只要 pyproject.toml / uv.lock 沒變，就不需要重裝套件
COPY pyproject.toml uv.lock ./

# ── 安裝 Python 套件 ────────────────────────────────────────
# --no-install-project：只裝依賴，不裝本地套件（prefectdemo）
# 因為 src/ 還沒被 COPY 進來，加這個 flag 才能正常 build
RUN uv sync --frozen --no-dev --no-install-project

# ── 複製專案程式碼 ───────────────────────────────────────────
COPY . .

# ── 讓容器內的 Prefect 能連回主機的 Server ──────────────────
# host.docker.internal = 主機 IP（OrbStack / Docker Desktop 均支援）
ENV PREFECT_API_URL=http://host.docker.internal:4200/api

# ── 預設指令（可被 Prefect 覆寫）────────────────────────────
CMD ["uv", "run", "python", "docker/flow.py"]
