FROM python:3.11-slim
WORKDIR /app

# Install curl for downloading static assets
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
COPY config_db.py .
COPY static/ static/

# Download external dependencies for offline/dark-site support
RUN mkdir -p static/vendor && \
    curl -sL "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js" -o static/vendor/chart.umd.min.js && \
    curl -sL "https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js" -o static/vendor/chartjs-adapter-date-fns.bundle.min.js && \
    curl -sL "https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&family=Space+Grotesk:wght@300;400;500;600;700&display=swap" -o static/vendor/fonts.css && \
    # Download the actual font files referenced in the CSS
    grep -oP 'url\(\K[^)]+' static/vendor/fonts.css | while read url; do \
        FILENAME=$(echo "$url" | sed 's/.*\///;s/?.*//' ); \
        curl -sL "$url" -o "static/vendor/${FILENAME}"; \
        sed -i "s|${url}|/static/vendor/${FILENAME}|g" static/vendor/fonts.css; \
    done

EXPOSE 5000
CMD ["gunicorn", "-w", "2", "-b", "0.0.0.0:5000", "app:app"]