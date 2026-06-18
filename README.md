# fastapi-storage

Django-style, loosely-coupled async file storage for FastAPI. Pluggable
backends (local filesystem now; S3 and Azure interfaces defined, GCS and
Dropbox planned) behind one stable `Storage` contract.

See `examples/app.py` for a working FastAPI app wiring this up.
