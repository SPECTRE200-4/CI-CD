FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# System dependencies (OpenCV + ML runtime)
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python tooling first (better caching)
RUN pip install --upgrade pip setuptools wheel

# Install PyTorch first (pinned is better in real systems)
RUN pip install \
    --index-url https://download.pytorch.org/whl/cpu \
    torch==2.3.0 torchvision==0.18.0

# Install application dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Install ultralytics separately (predictable layer)
RUN pip install ultralytics


# Copy application code
COPY app.py .

# Pre-download model (optional but now isolated logically)
RUN python -c "from ultralytics import YOLO; YOLO('yolo11n.pt')"

CMD ["python", "app.py"]
