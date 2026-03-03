FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

ENV PYTHONPATH=/app

COPY backend/pipeline/notification/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY backend ./backend

CMD ["functions-framework", \
     "--source=backend/pipeline/notification/send_notification.py", \
     "--target=send_notification", \
     "--signature-type=cloudevent", \
     "--port=8080"]
