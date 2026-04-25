"""Upload all playground files (backend + frontend) to HF Space."""

import shutil
import tempfile
from pathlib import Path

from huggingface_hub import HfApi

REPO_ID = "Praneshrajan15/dataforge-playground"
ROOT = Path(r"c:\Users\Pranesh\OneDrive\Music\OpenEnv RL\data_quality_env")

DOCKERFILE = r"""# DataForge Playground - Multi-stage Docker build for HF Spaces.
FROM python:3.12-slim AS builder
WORKDIR /build
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ && \
    rm -rf /var/lib/apt/lists/*
COPY playground/api/requirements.txt /build/requirements.txt
RUN pip install --no-cache-dir -r /build/requirements.txt
COPY pyproject.toml /build/dataforge_src/pyproject.toml
COPY README_MAIN.md /build/dataforge_src/README.md
COPY dataforge/ /build/dataforge_src/dataforge/
COPY constitutions/ /build/dataforge_src/constitutions/
RUN pip install --no-cache-dir /build/dataforge_src

FROM python:3.12-slim
RUN useradd -m -u 1000 user
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /build/dataforge_src/constitutions /usr/local/lib/python3.12/site-packages/constitutions
COPY playground/api/app.py /home/user/app/app.py
COPY playground/api/samples/ /home/user/app/samples/
COPY playground/web/ /home/user/app/web/
USER user
WORKDIR /home/user/app
EXPOSE 7860
ENV PORT=7860
ENV DATAFORGE_PLAYGROUND_DEV=0
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "7860", "--workers", "1", "--timeout-keep-alive", "5"]
"""


def main() -> None:
    api = HfApi()

    with tempfile.TemporaryDirectory() as tmpdir:
        stage = Path(tmpdir) / "stage"
        stage.mkdir()

        # Dockerfile
        (stage / "Dockerfile").write_text(DOCKERFILE, encoding="utf-8")

        # HF README
        shutil.copy2(ROOT / "playground/api/README.md", stage / "README.md")

        # playground/api/
        api_dir = stage / "playground" / "api"
        api_dir.mkdir(parents=True)
        shutil.copy2(ROOT / "playground/api/app.py", api_dir / "app.py")
        shutil.copy2(ROOT / "playground/api/requirements.txt", api_dir / "requirements.txt")
        shutil.copytree(ROOT / "playground/api/samples", api_dir / "samples")

        # playground/web/ (frontend)
        shutil.copytree(
            ROOT / "playground/web",
            stage / "playground" / "web",
            ignore=shutil.ignore_patterns("DEPLOY.md"),
        )

        # dataforge source
        shutil.copytree(
            ROOT / "dataforge",
            stage / "dataforge",
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )

        # pyproject.toml + README for pip install
        shutil.copy2(ROOT / "pyproject.toml", stage / "pyproject.toml")
        shutil.copy2(ROOT / "README.md", stage / "README_MAIN.md")

        # constitutions
        shutil.copytree(
            ROOT / "constitutions",
            stage / "constitutions",
            ignore=shutil.ignore_patterns("__pycache__"),
        )

        # Count files
        files = [f for f in stage.rglob("*") if f.is_file()]
        print(f"Uploading {len(files)} files to {REPO_ID}...")

        api.upload_folder(
            folder_path=str(stage),
            repo_id=REPO_ID,
            repo_type="space",
            commit_message="feat: serve frontend from HF Space (same-origin)",
        )
        print("Upload complete! Space will rebuild.")


if __name__ == "__main__":
    main()
